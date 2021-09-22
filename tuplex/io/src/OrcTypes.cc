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
    } else {
        throw std::runtime_error("Tuplex row type unable to be mapped to Orc row type");
    }
}

python::Type orcRowTypeToTuplex(const ::orc::Type &rowType, std::vector<bool> &columnHasNull) {
    using namespace ::orc;
    std::vector<python::Type> types;
    for (uint64_t i = 0; i < rowType.getSubtypeCount(); i++) {
        auto hasNull = columnHasNull.at(i);
        auto el = orcTypeToTuplex(*rowType.getSubtype(i), hasNull);
        if (hasNull) {
            types.push_back(python::Type::makeOptionType(el));
        } else {
            types.push_back(el);
        }
    }
    return python::Type::makeTupleType(types);
}

python::Type orcTypeToTuplex(const ::orc::Type &type, bool hasNull) {
    switch (type.getKind()) {
        case ::orc::BOOLEAN: {
            if (hasNull) {
                return python::Type::makeOptionType(python::Type::BOOLEAN);
            } else {
                return python::Type::BOOLEAN;
            }
        }
        case ::orc::DATE:
        case ::orc::TIMESTAMP:
        case ::orc::TIMESTAMP_INSTANT:
        case ::orc::BYTE:
        case ::orc::SHORT:
        case ::orc::INT:
        case ::orc::LONG: {
            if (hasNull) {
                return python::Type::makeOptionType(python::Type::I64);
            } else {
                return python::Type::I64;
            }
        }
        case ::orc::FLOAT:
        case ::orc::DOUBLE: {
            if (hasNull) {
                return python::Type::makeOptionType(python::Type::F64);
            } else {
                return python::Type::F64;
            }
        }
        case ::orc::BINARY:
        case ::orc::VARCHAR:
        case ::orc::CHAR:
        case ::orc::STRING: {
            if (hasNull) {
                return python::Type::makeOptionType(python::Type::STRING);
            } else {
                return python::Type::STRING;
            }
        }
        case ::orc::LIST: {
            auto elementType = orcTypeToTuplex(*type.getSubtype(0), hasNull);
            if (hasNull) {
                return python::Type::makeOptionType(python::Type::makeListType(elementType));
            } else {
                return python::Type::makeListType(elementType);
            }
        }
        case ::orc::MAP: {
            auto keyType = orcTypeToTuplex(*type.getSubtype(0), hasNull);
            auto valueType = orcTypeToTuplex(*type.getSubtype(1), hasNull);
            if (hasNull) {
                return python::Type::makeOptionType(python::Type::makeDictionaryType(keyType, valueType));
            } else {
                return python::Type::makeDictionaryType(keyType, valueType);
            }
        }
        case ::orc::STRUCT: {
            std::vector<python::Type> types;
            for (uint64_t i = 0; i < type.getSubtypeCount(); i++) {
                auto el = orcTypeToTuplex(*type.getSubtype(i), hasNull);
                types.push_back(el);
            }
            if (hasNull) {
                return python::Type::makeOptionType(python::Type::makeTupleType(types));
            } else {
                return python::Type::makeTupleType(types);
            }
        }
        default:
            throw std::runtime_error("Orc row type: " + type.toString() + " unable to be converted to Tuplex type");
    }
}

}}
