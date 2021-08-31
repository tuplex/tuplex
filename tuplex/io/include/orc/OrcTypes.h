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
* ::orc::DECIMAL -> python::F64
* ::orc::VARCHAR -> python::STRING
* ::orc::CHAR -> python::STRING
*
* The following ORC cases are currently undefined:
* ::orc::BINARY
* ::orc::TIMESTAMP
* ::orc::UNION
* ::orc::DATE
*
* @param rowType: Orc row type
* @param orcBatch: Batch associated with type
* @return Python Type object
*/
python::Type orcRowTypeToTuplex(const ::orc::Type &rowType, ::orc::ColumnVectorBatch *orcBatch);

/*!
* Takes a Tuplex row type and converts it to its corresponding
* Orc row type.
*
* The following Tuplex cases are currently undefined:
* python::UNKNOWN
* python::VOID
* python::PYOBJECT
* python::MATCHOBJECT
* python::RANGE
* python::MODULE
*
* @param rowType: Tuplex row type
* @return ORC type pointer
*/
ORC_UNIQUE_PTR<::orc::Type> tuplexRowTypeToOrcType(const python::Type &rowType, const std::vector<std::string> &columns = {});

}}

#endif //TUPLEX_ORCTYPES_H
