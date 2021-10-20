//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_SERIALIZER_H
#define TUPLEX_SERIALIZER_H

#include <Base.h>
#include <Schema.h>
#include <Tuple.h>
#include <List.h>
#ifdef BUILD_WITH_AWS
#include <aws/core/external/cjson/cJSON.h>
#else
#include <cJSON.h>
#endif
#include "optional.h"


// Note: exchange vector<bool> to https://www.boost.org/doc/libs/1_72_0/libs/dynamic_bitset/dynamic_bitset.html

namespace tuplex {

    // dummy class to represent a NullValue type.
    class NullType {};

    /*!
     * a helper class for a growing buffer
     */
    class Buffer {
    private:
        size_t _growthConstant;
        void *_buffer;
        size_t _bufferSize;
        size_t _bufferCapacity;
    public:
        Buffer(const size_t growthConstant) : _growthConstant(growthConstant), _buffer(nullptr), _bufferSize(0), _bufferCapacity(0) {
            assert(_growthConstant > 0);
        }

        ~Buffer() {
            if(_buffer)
                free(_buffer);
        }

        void provideSpace(const size_t numBytes);

        void* buffer() { return _buffer; }

        void* ptr() const { static_assert(sizeof(unsigned char) == 1, "byte type must be 1 byte wide"); assert(_buffer); return (unsigned char*)_buffer + _bufferSize; }
        void movePtr(const size_t numBytes) { _bufferSize += numBytes; }
        size_t size() const { return _bufferSize; }
        size_t capacity() const { return _bufferCapacity; }
        void reset() { _bufferSize = 0; }
    };

    /*!
     * helper class to serialize data. So far only primitive types are supported. Internal memory representation is identical
     * to Spark's Tungsten format as of now
     */
    class Serializer {
    private:
        bool _autoSchema; //! auto infer schema from calls to append functions
        Schema _schema;
        std::vector<python::Type> _types; // types used to auto schema inference.
        void fixSchema();
        int _col;

        Buffer _fixedLenFields; //! flattened fixed len fields
        Buffer _varLenFields; //! flattened var len fields

        // auto inferred fields & types
        std::vector<bool> _isVarField; //!flattened info whether field is varf
        std::vector<size_t> _varLenFieldOffsets; // only for var fields!

        std::vector<bool> _requiresBitmap; //! whether field can be null and thus info needs to be stored in bitmap.
        std::vector<bool> _isNull; //! indicates whether option is null or not

        static const int _bufferGrowthConstant = 256; // grow by 256 bytes

        int numFields() { return _isVarField.size(); }
        int numVarFields() { return _varLenFieldOffsets.size(); }

        Serializer& appendWithoutInference(const bool b);
        Serializer& appendWithoutInference(const int64_t i);
        Serializer& appendWithoutInference(const double d);
        Serializer& appendWithoutInference(const std::string &str);
        Serializer& appendWithoutInference(const NullType &n);
        Serializer& appendWithoutInference(const List &l);

        // nullable types
        Serializer& appendWithoutInference(const option<bool>& b);
        Serializer& appendWithoutInference(const option<int64_t>& i);
        Serializer& appendWithoutInference(const option<double>& d);
        Serializer& appendWithoutInference(const option<std::string> &str);
        Serializer& appendWithoutInference(const option<List> &list, const python::Type &listType);

        Serializer& appendWithoutInference(const uint8_t* buf, size_t bufSize);

        Serializer& appendWithoutInference(const Field f);

        inline bool hasSchemaVarLenFields() const {
            // from _isVarLenField, if any element is set to true return true
            return std::any_of(_isVarField.begin(), _isVarField.end(), [](bool b) { return b; });
        }
    public:
        Serializer(bool autoSchema = true) : _autoSchema(autoSchema),
                                             _fixedLenFields(_bufferGrowthConstant),
                                             _varLenFields(_bufferGrowthConstant), _col(0)   {}

        Serializer& reset();

        // general case: options!
        Serializer& append(const option<bool>& b);
        Serializer& append(const option<int64_t>& i);
        Serializer& append(const option<double>& d);
        Serializer& append(const option<std::string>& str);
        Serializer& append(const option<std::string>& dict, const python::Type& dictType);

        Serializer& append(const option<const char*>& str) { return append(str.has_value() ? std::string(str.value()) : option<std::string>::none); }
        Serializer& append(const option<float>& f) { return  append(f.has_value() ? static_cast<double>(f.value()) : option<double>::none); }
        Serializer& append(const option<int>& i) { return append(i.has_value() ? static_cast<int64_t>(i.value()) : option<int64_t>::none); }
        Serializer& appendEmptyTupleOption(const Field &f);
        Serializer& append(const option<Tuple>& t);
        Serializer& append(const option<List>& list, python::Type listType);

        // data fields which are not nullable
        Serializer& append(const bool b);
        Serializer& append(const int64_t i);
        Serializer& append(const double d);
        Serializer& append(const std::string &str);
        Serializer& append(const std::string &dict, const python::Type& dictType);
        Serializer& append(const NullType &n);

        Serializer& append(const char* str) { return append(std::string(str)); }
        Serializer& append(const float f) { return append(static_cast<double>(f)); }
        Serializer& append(const int i) { return append(static_cast<int64_t>(i)); }

        Serializer& append(const List &l);
        Serializer& append(const Tuple &t);

        Serializer& appendObject(const uint8_t* buf, size_t bufSize);

        Serializer& appendNull();

        // only define append for long when long and int64_t are not the same to avoid overload error
        template<class T=long>
        typename std::enable_if<!std::is_same<int64_t, T>::value, Serializer&>::type append(const T l) { return append(static_cast<int64_t>(l)); }

        Schema getSchema()  { fixSchema(); return _schema; }

        /*!
         * serializes data to the buffer if there is enough capacity left
         * @param ptr memory location to where to serialize data to
         * @param capacityLeft how many bytes can be written after ptr?
         * @return number of bytes written, 0 if insufficient space or if no data was present.
         */
        size_t serialize(void *ptr, const size_t capacityLeft);

        /*!
         * provides the current length a buffer must have to store the contents passed to the serializer
         * @return length that serialized contents would have
         */
        size_t length();
    };

    class Deserializer {
    private:

        // the original (nested) schema associated with the data...
        Schema _schema;

        python::Type _flattenedRowType; ///! nesting is resolved by flattening everything out.

        std::vector<bool> _isVarLenField;
        std::vector<bool> _requiresBitmap;

        // vars to setup from schema
        std::unordered_map<size_t, size_t> _idxMap; // logical to physical map
        size_t _numSerializedFields;

        void *_buffer;

        bool hasSchemaVarLenFields() const;

        inline int numFields() const { return _isVarLenField.size(); }

        inline size_t numSerializedFields() const { return _numSerializedFields; }
        size_t logicalToPhysicalIndex(size_t logicalIdx) const { return _idxMap.at(logicalIdx); }


        inline bool hasBitmap() const {
            return std::any_of(_requiresBitmap.begin(), _requiresBitmap.end(), [](const bool b) { return b; });
        }

    public:
        inline bool isNull(int col) const {
            assert(0 <= col && col < _isVarLenField.size());
            assert(_buffer);
            if(!hasBitmap())
                return false;

            // only options are encoded in bitmap, i.e. have to translate indexing here!
            unsigned ncol = 0;
            for(int i = 0; i < col; ++i)
                ncol += _requiresBitmap[i];

            // check bitmap
            auto *ibuf = reinterpret_cast<uint64_t*>(_buffer);
            bool is_null = ibuf[ncol / 64] & (1UL << (ncol % 64));
            return is_null;
        }

        Deserializer(const Schema& schema);

        ~Deserializer() {
            if(_buffer)
                free(_buffer);
        }

        // for test purposes...
        size_t inferLength(const void *ptr) const;

        Deserializer& deserialize(const void* ptr, const int capacityLeft);

        // return elements from flattened structure...
        bool        getBool(const int col) const;
        int64_t     getInt(const int col) const;
        double      getDouble(const int col) const;
        std::string getString(const int col) const;
        std::string getDictionary(const int col) const;
        List        getList(const int col) const;

        const uint8_t* getPtr(const int col) const;
        size_t getSize(const int col) const;

        /*!
         * deserializes buffer and returns (nested) tuple structure with all fields.
         * @return
         */
        Tuple      getTuple() const;

        size_t rowSize() const { assert(_buffer); return inferLength(_buffer); }
        const void* buffer() const { assert(_buffer); return _buffer; }

        Schema getSchema() const { return _schema; }
    };


    // mapping c++ types to python Types
    inline python::Type deductType(const bool b) { return python::Type::BOOLEAN; }
    inline python::Type deductType(const int16_t i) { return python::Type::I64; }
    inline python::Type deductType(const int32_t i) { return python::Type::I64; }
    inline python::Type deductType(const int64_t i) { return python::Type::I64; }
    inline python::Type deductType(const float f) { return python::Type::F64; }
    inline python::Type deductType(const double d) { return python::Type::F64; }

#undef STRING
    inline python::Type deductType(const std::string s) { return python::Type::STRING; }
    inline python::Type deductType(const char* s) { return python::Type::STRING; }

    /*!
     * determine the serialization schema for varargs, always returns a tuple type.
     * @param ...
     * @return tuple type of row, e.g. if elements are i64, this will return (i64)
     */
    template<class... Args> Schema serializationSchema(Args... args) {
        std::vector<python::Type> v = {deductType(args)...};
        return Schema(Schema::MemoryLayout::UNKNOWN, python::TypeFactory::instance().createOrGetTupleType(v));
    }

}

#endif //TUPLEX_SERIALIZER_H