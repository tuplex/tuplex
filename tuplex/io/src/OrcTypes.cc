//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <orc/OrcTypes.h>

namespace tuplex { namespace orc {

ORC_UNIQUE_PTR<::orc::Type> tuplexRowTypeToOrcType(const python::Type &rowType, const std::vector<std::string> &columns) {
    if (rowType == python::Type::I64) {
        return ::orc::createPrimitiveType(::orc::LONG);
    } else if (rowType == python::Type::F64) {
        return ::orc::createPrimitiveType(::orc::DOUBLE);
    } else if (rowType == python::Type::STRING) {
        return ::orc::createPrimitiveType(::orc::STRING);
    } else if (rowType == python::Type::BOOLEAN) {
        return ::orc::createPrimitiveType(::orc::BOOLEAN);
    } else if (rowType == python::Type::NULLVALUE) {
        return ::orc::createPrimitiveType(::orc::STRING);
    } else if (rowType.isTupleType() || rowType == python::Type::EMPTYTUPLE) {
        auto structType = ::orc::createStructType();
        if (!columns.empty()) {
            for (auto i = 0; i < rowType.parameters().size(); i++) {
                structType->addStructField(columns.at(i), tuplexRowTypeToOrcType(rowType.parameters()[i]));
            }
        } else {
            for (const auto& el : rowType.parameters()) {
                structType->addStructField("", tuplexRowTypeToOrcType(el));
            }
        }
        return structType;
    } else if (rowType.isDictionaryType() || rowType == python::Type::EMPTYDICT) {
        auto keyType = tuplexRowTypeToOrcType(rowType.keyType(), {});
        auto valueType = tuplexRowTypeToOrcType(rowType.valueType(), {});
        return ::orc::createMapType(std::move(keyType), std::move(valueType));
    } else if (rowType.isListType() || rowType == python::Type::EMPTYLIST) {
        auto elementType = tuplexRowTypeToOrcType(rowType.elementType());
        return ::orc::createListType(std::move(elementType));
    } else if (rowType.isOptionType()) {
        return tuplexRowTypeToOrcType(rowType.elementType());
    }
    throw std::runtime_error("Tuplex row type unable to be mapped to Orc row type");
}

python::Type orcRowTypeToTuplex(const ::orc::Type &rowType, ::orc::ColumnVectorBatch *orcBatch) {
    switch (rowType.getKind()) {
        case ::orc::BOOLEAN: {
            auto longBatch = static_cast<::orc::LongVectorBatch *>(orcBatch);
            if (longBatch->hasNulls) {
                return python::Type::makeOptionType(python::Type::BOOLEAN);
            } else {
                return python::Type::BOOLEAN;
            }
        }
        case ::orc::BYTE:
        case ::orc::SHORT:
        case ::orc::INT:
        case ::orc::LONG: {
            auto longBatch = static_cast<::orc::LongVectorBatch *>(orcBatch);
            if (longBatch->hasNulls) {
                return python::Type::makeOptionType(python::Type::I64);
            } else {
                return python::Type::I64;
            }
        }
        case ::orc::FLOAT:
        case ::orc::DOUBLE: {
            auto doubleBatch = static_cast<::orc::DoubleVectorBatch *>(orcBatch);
            if (doubleBatch->hasNulls) {
                return python::Type::makeOptionType(python::Type::F64);
            } else {
                return python::Type::F64;
            }
        }
        case ::orc::STRING: {
            auto strBatch = static_cast<::orc::StringVectorBatch *>(orcBatch);
            if (strBatch->hasNulls) {
                return python::Type::makeOptionType(python::Type::STRING);
            } else {
                return python::Type::STRING;
            }
        }
        case ::orc::LIST: {
            auto listBatch = static_cast<::orc::ListVectorBatch *>(orcBatch);
            auto elementType = orcRowTypeToTuplex(*rowType.getSubtype(0), listBatch->elements.get());
            if (listBatch->hasNulls) {
                return python::Type::makeOptionType(python::Type::makeListType(elementType));
            } else {
                return python::Type::makeListType(elementType);
            }
        }
        case ::orc::MAP: {
            auto mapBatch = static_cast<::orc::MapVectorBatch *>(orcBatch);
            auto keyType = orcRowTypeToTuplex(*rowType.getSubtype(0), mapBatch->keys.get());
            auto valueType = orcRowTypeToTuplex(*rowType.getSubtype(1), mapBatch->elements.get());
            if (mapBatch->hasNulls) {
                return python::Type::makeOptionType(python::Type::makeDictionaryType(keyType, valueType));
            } else {
                return python::Type::makeDictionaryType(keyType, valueType);
            }
        }
        case ::orc::STRUCT: {
            auto structBatch = static_cast<::orc::StructVectorBatch *>(orcBatch);
            std::vector<python::Type> types;
            for (uint64_t i = 0; i < rowType.getSubtypeCount(); i++) {
                auto el = orcRowTypeToTuplex(*rowType.getSubtype(i), structBatch->fields[i]);
                types.push_back(el);
            }
            if (structBatch->hasNulls) {
                return python::Type::makeOptionType(python::Type::makeTupleType(types));
            } else {
                return python::Type::makeTupleType(types);
            }
        }
        case ::orc::VARCHAR:
        case ::orc::CHAR: {
            auto strBatch = static_cast<::orc::StringVectorBatch *>(orcBatch);
            if (strBatch->hasNulls) {
                return python::Type::makeOptionType(python::Type::STRING);
            } else {
                return python::Type::STRING;
            }
        }
        default:
            throw std::runtime_error("Orc row type unable to be converted to Tuplex type");
    }
}
}}
