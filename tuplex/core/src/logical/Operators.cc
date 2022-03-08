//
// Created by Leonhard Spiegelberg on 3/8/22.
//

#include <logical/Operators.h>

// do here all Cereal type registration
// CEREAL
#include "cereal/types/polymorphic.hpp"
#include "cereal/archives/binary.hpp"
#include "cereal/archives/portable_binary.hpp"
namespace tuplex {
// not required according to cereal doc, yet register despite
CEREAL_REGISTER_TYPE_WITH_NAME(LogicalOperator, "LogicalOperator");
CEREAL_REGISTER_TYPE_WITH_NAME(FileInputOperator, "FileInputOperator");
CEREAL_REGISTER_TYPE_WITH_NAME(UDFOperator, "UDFOperator");
CEREAL_REGISTER_TYPE_WITH_NAME(MapOperator, "MapOperator");

CEREAL_REGISTER_POLYMORPHIC_RELATION(LogicalOperator, UDFOperator)
CEREAL_REGISTER_POLYMORPHIC_RELATION(LogicalOperator, FileInputOperator)
CEREAL_REGISTER_POLYMORPHIC_RELATION(UDFOperator, MapOperator)

// Potentially necessary if no explicit reference
// to objects in myclasses.cpp will take place
// from other translation units
CEREAL_REGISTER_DYNAMIC_INIT(Operators)

}
