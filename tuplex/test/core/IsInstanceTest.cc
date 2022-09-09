//
// Created by Leonhard Spiegelberg on 9/8/22.
//

#include "TestUtils.h"
#include "JsonStatistic.h"

#include <AccessPathVisitor.h>

class IsInstance : public PyTest {};

TEST_F(IsInstance, BasicTyping) {
    // this is a basic test for isinstance covering the correct type deduction

    using namespace tuplex;

    // this also requires to implement/add type objects.
    // i.e. Type[str] => this gives a type object for type str.
    // these should not be serializable...
    // but may occur within the code.
    // e.g. type(obj) => should create type object! => this can be simply assigned.
    // no need to compile that since we're restricted to them internally.


    // isinstance has multiple supported syntaxes
    // isinstance(object, classinfo)
    //Return True if the object argument is an instance of the
    // classinfo argument, or of a (direct, indirect, or virtual)
    // subclass thereof. If object is not an object of the given type,
    // the function always returns False. If classinfo is a tuple of
    // type objects (or recursively, other such tuples) or a Union
    // Type of multiple types, return True if object is an instance
    // of any of the types. If classinfo is not a type or tuple of types
    // and such tuples, a TypeError exception is raised.
    //
    //Changed in version 3.10: classinfo can be a Union Type.
    //
    //issubclass(class, classinfo)
    //Return True if class is a subclass (direct, indirect, or virtual)
    // of classinfo. A class is considered a subclass of itself. classinfo
    // may be a tuple of class objects (or recursively, other such tuples)
    // or a Union Type, in which case return True if class is a subclass
    // of any entry in classinfo. In any other case, a TypeError exception
    // is raised.
    //
    //Changed in version 3.10: classinfo can be a Union Type.

    // basic check for type objects
    {
        UDF udf("lambda x: (str, bool, int)");
        auto input_type = python::Type::I64;
        udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(input_type)));
        auto ret_type = udf.getAnnotatedAST().getReturnType();
        auto output_type = python::Type::makeTupleType({python::Type::makeTypeObjectType(python::Type::STRING),
                                                        python::Type::makeTypeObjectType(python::Type::BOOLEAN),
                                                        python::Type::makeTypeObjectType(python::Type::I64),});
        EXPECT_EQ(ret_type.desc(), output_type.desc());
    }

    // basic check for type objects
    {
        UDF udf("lambda str: (str, bool, int)");
        auto input_type = python::Type::I64;
        udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(input_type)));
        auto ret_type = udf.getAnnotatedAST().getReturnType();
        auto output_type = python::Type::makeTupleType({python::Type::I64,
                                                        python::Type::makeTypeObjectType(python::Type::BOOLEAN),
                                                        python::Type::makeTypeObjectType(python::Type::I64),});
        EXPECT_EQ(ret_type.desc(), output_type.desc());
    }

    return;

    // start now with isinstance typing
    {
        UDF udf("lambda x: x if isinstance(x, str) else str(x)");
        auto input_type = python::Type::I64;
        udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(input_type)));
        auto ret_type = udf.getAnnotatedAST().getReturnType();
        EXPECT_EQ(ret_type, python::Type::BOOLEAN);
    }

    {
        UDF udf("lambda x: isinstance(x, 42)");
        auto input_type = python::Type::I64;
        udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(input_type)));
        auto ret_type = udf.getAnnotatedAST().getReturnType();
        EXPECT_EQ(ret_type, python::TypeFactory::instance().getByName("TypeError"));
    }

    // todo: could also allow use of something like typing.Optional[...] or typing.Tuple[...] and so on...
    // further, could allow for callable[...]
    // and also operators like | for type unions etc.
}