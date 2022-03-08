//
// Created by Leonhard Spiegelberg on 3/8/22.
//
// this header is a collection of all available operators and contains the serialization logic/type registry for Cereal

#ifndef TUPLEX_OPERATORS_H
#define TUPLEX_OPERATORS_H

#include "AggregateOperator.h"
#include "CacheOperator.h"
#include "ExceptionOperator.h"
#include "FileInputOperator.h"
#include "FileOutputOperator.h"
#include "FilterOperator.h"
#include "IgnoreOperator.h"
#include "JoinOperator.h"
#include "LogicalOperator.h"
#include "MapColumnOperator.h"
#include "MapOperator.h"
#include "ParallelizeOperator.h"
#include "ResolveOperator.h"
#include "TakeOperator.h"
#include "UDFOperator.h"
#include "WithColumnOperator.h"

// CEREAL
#include "cereal/types/polymorphic.hpp"
#include "cereal/archives/binary.hpp"
#include "cereal/archives/portable_binary.hpp"

// not required according to cereal doc, yet register despite
CEREAL_REGISTER_TYPE_WITH_NAME(tuplex::LogicalOperator, "LogicalOperator");
CEREAL_REGISTER_TYPE_WITH_NAME(tuplex::FileInputOperator, "FileInputOperator");
CEREAL_REGISTER_TYPE_WITH_NAME(tuplex::UDFOperator, "UDFOperator");
CEREAL_REGISTER_TYPE_WITH_NAME(tuplex::MapOperator, "MapOperator");

CEREAL_REGISTER_POLYMORPHIC_RELATION(tuplex::LogicalOperator, tuplex::UDFOperator)
CEREAL_REGISTER_POLYMORPHIC_RELATION(tuplex::LogicalOperator, tuplex::FileInputOperator)
CEREAL_REGISTER_POLYMORPHIC_RELATION(tuplex::UDFOperator, tuplex::MapOperator)

// Potentially necessary if no explicit reference
// to objects in myclasses.cpp will take place
// from other translation units
CEREAL_REGISTER_DYNAMIC_INIT(tuplex_classes)

#endif //TUPLEX_OPERATORS_H
