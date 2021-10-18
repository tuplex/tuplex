//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ORCTYPES_H
#define TUPLEX_ORCTYPES_H

#include <TypeSystem.h>
#include <orc/Type.hh>
#include <orc/OrcFile.hh>

namespace tuplex { namespace orc {

/*!
* Takes an Orc row type and converts it to its corresponding
* Tuplex row type.
*
* The following mappings exist for special cases:
* orc::DECIMAL -> tuplex::F64
* orc::VARCHAR -> tuplex::String
* orc::CHAR -> tuplex::String
* orc::TIMESTAMP -> tuplex::I64
* orc::DATE -> tuplex::I64
* orc::BINARY -> tuplex::String
*
* The following ORC cases are currently unsupported:
* orc::UNION
* orc::DECIMAL
*
* @param rowType: Orc row type
* @param columnHasNull: If each column has null values
* @return Python Type object
*/
python::Type orcRowTypeToTuplex(const ::orc::Type &rowType, std::vector<bool> &columnHasNull);

/*!
* Takes an Orc type and converts it to its corresponding
* Tuplex type.
*
* The following mappings exist for special cases:
* orc::DECIMAL -> tuplex::F64
* orc::VARCHAR -> tuplex::String
* orc::CHAR -> tuplex::String
* orc::TIMESTAMP -> tuplex::I64
* orc::DATE -> tuplex::I64
* orc::BINARY -> tuplex::String
*
* The following ORC cases are currently unsupported:
* orc::UNION
* orc::DECIMAL
*
* @param type: Orc row type
* @param hasNull: If the type will be optional
* @return Python Type object
*/
python::Type orcTypeToTuplex(const ::orc::Type &type, bool hasNull);

/*!
* Takes a Tuplex row type and converts it to its corresponding
* Orc row type.
*
* The following Tuplex cases are currently undefined:
* tuplex::UNKNOWN
* tuplex::VOID
* tuplex::PYOBJECT
* tuplex::MATCHOBJECT
* tuplex::RANGE
* tuplex::MODULE
*
* @param rowType: Tuplex row type
* @return ORC type pointer
*/
ORC_UNIQUE_PTR<::orc::Type> tuplexRowTypeToOrcType(const python::Type &rowType, const std::vector<std::string> &columns = {});

}}

#endif //TUPLEX_ORCTYPES_H
