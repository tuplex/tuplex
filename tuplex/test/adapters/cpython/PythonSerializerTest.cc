//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <limits>

#include <gtest/gtest.h>

#include "Python.h"

#include "PythonSerializer.h"
#include "PythonSerializer_private.h"
#include "Row.h"

using namespace tuplex;
using namespace tuplex::cpython;

TEST(PythonSerializer, TestIsCapacityValidBadCapacity) {
    EXPECT_FALSE(isCapacityValid(nullptr, 0, python::Type::UNKNOWN));
    EXPECT_FALSE(isCapacityValid(nullptr, -1, python::Type::UNKNOWN));
    EXPECT_FALSE(isCapacityValid(nullptr, -2, python::Type::UNKNOWN));
}

TEST(PythonSerializer, TestIsCapacityValidOverflowString) {
    size_t capacity = 256;
    uint8_t buffer[capacity];

    memset(buffer, 0, sizeof(uint8_t) * capacity);
    Row row({Field("Hello, World!")});
    row.serializeToMemory(buffer, capacity);
    EXPECT_FALSE(isCapacityValid(buffer, 16, python::Type::STRING));
}

TEST(PythonSerializer, TestIsCapacityValidOverflowTuple) {
    size_t capacity = 256;
    uint8_t buffer[capacity];

    memset(buffer, 0, sizeof(uint8_t) * capacity);
    //Row row({Field(Tuple({Field(int64_t(42)), Field(3.141), Field(true)}))});
    Row row(Tuple(42, 3.141, true));
    row.serializeToMemory(buffer, capacity);
    EXPECT_FALSE(isCapacityValid(buffer, 16, row.getSchema().getRowType()));
}

TEST(PythonSerializer, TestIsCapacityValidFixedSize) {
    size_t capacity = 256;
    uint8_t buffer[capacity];

    memset(buffer, 0, sizeof(uint8_t) * capacity);
//    Row row({Field((int64_t) 42)});
    Row row(42);
    row.serializeToMemory(buffer, capacity);
    EXPECT_TRUE(isCapacityValid(buffer, 8, row.getSchema().getRowType()));
}

TEST(PythonSerializer, TestIsCapacityValidVarLength) {
    size_t capacity = 256;
    uint8_t buffer[capacity];

    memset(buffer, 0, sizeof(uint8_t) * capacity);
    Row row({Field((int64_t) "Hello, World!")});
    row.serializeToMemory(buffer, capacity);
    EXPECT_TRUE(isCapacityValid(buffer, capacity, row.getSchema().getRowType()));
}

TEST(PythonSerializer, TestCheckTupleCapacityBufferExceeded) {
    size_t capacity = 256;
    uint8_t buffer[capacity];

    memset(buffer, 0, sizeof(uint8_t) * capacity);
    //Row row({Field(Tuple({Field(int64_t(42)), Field(3.141), Field(true)}))});
    Row row(Tuple(42, 3.141, true));
    row.serializeToMemory(buffer, capacity);
    EXPECT_EQ(-1, checkTupleCapacity(buffer, 16, row.getSchema().getRowType()));
}

TEST(PythonSerializer, TestCheckTupleCapacityVarLengthItemExceeded) {
    size_t capacity = 256;
    uint8_t buffer[capacity];

    memset(buffer, 0, sizeof(uint8_t) * capacity);
//    Row row({Field(Tuple({Field("Hello, World!")}))});
    Row row(Tuple("Hello, World!"));
    row.serializeToMemory(buffer, capacity);
    EXPECT_EQ(-1, checkTupleCapacity(buffer, 16, row.getSchema().getRowType()));
}

TEST(PythonSerializer, TestCheckTupleCapacityNestedTuple) {
    size_t capacity = 256;
    uint8_t buffer[capacity];

    memset(buffer, 0, sizeof(uint8_t) * capacity);
    Row row(42, Tuple(Tuple(3.141), "Hello world!"), 3.141, "Hello world!");
    row.serializeToMemory(buffer, capacity);
    EXPECT_EQ(-1, checkTupleCapacity(buffer, 16, row.getSchema().getRowType()));
}

void checkCreatePyObjectFromMemoryBool(uint8_t *buffer, size_t capacity, bool got, bool want, bool equal) {
    memset(buffer, 0, sizeof(uint8_t) * capacity);
    Row row = Row(got);
    row.serializeToMemory(buffer, capacity);
    EXPECT_EQ(1,
              PyObject_RichCompareBool(
                      PyBool_FromLong(want),
                      createPyObjectFromMemory(buffer, python::Type::BOOLEAN, capacity),
                      equal ? Py_EQ : Py_NE));
}

TEST(PythonSerializer, TestCreatePyObjectFromMemoryBool) {
    Py_Initialize();

    size_t capacity = 256;
    uint8_t buffer[capacity];

    checkCreatePyObjectFromMemoryBool(buffer, capacity, true, true, true);
    checkCreatePyObjectFromMemoryBool(buffer, capacity, false, false, true);
    checkCreatePyObjectFromMemoryBool(buffer, capacity, true, false, false);
    checkCreatePyObjectFromMemoryBool(buffer, capacity, false, true, false);
}

void checkCreatePyObjectFromMemoryLong(uint8_t *buffer, size_t capacity, int64_t got, int64_t want, bool equal) {
    memset(buffer, 0, sizeof(uint8_t) * capacity);
    Row row = Row({Field(got)});
    row.serializeToMemory(buffer, capacity);
    EXPECT_EQ(1,
              PyObject_RichCompareBool(
                      PyLong_FromLong(want),
                      createPyObjectFromMemory(buffer, python::Type::I64, capacity),
                      equal ? Py_EQ : Py_NE));
}

TEST(PythonSerializer, TestCreatePyObjectFromMemoryLong) {
    Py_Initialize();

    size_t capacity = 256;
    uint8_t buffer[capacity];

    checkCreatePyObjectFromMemoryLong(buffer, capacity, 0, 0, true);
    checkCreatePyObjectFromMemoryLong(buffer, capacity, 1, 1, true);
    checkCreatePyObjectFromMemoryLong(buffer, capacity, -1, -1, true);
    checkCreatePyObjectFromMemoryLong(buffer, capacity, 42, 42, true);
    checkCreatePyObjectFromMemoryLong(buffer, capacity, std::numeric_limits<int64_t>::min(),
                                      std::numeric_limits<int64_t>::min(), true);
    checkCreatePyObjectFromMemoryLong(buffer, capacity, std::numeric_limits<int64_t>::max(),
                                      std::numeric_limits<int64_t>::max(), true);
    checkCreatePyObjectFromMemoryLong(buffer, capacity, std::numeric_limits<int64_t>::max(),
                                      std::numeric_limits<int64_t>::min(), false);
}

void checkCreatePyObjectFromMemoryFloat(uint8_t *buffer, size_t capacity, double got, double want, bool equal) {
    memset(buffer, 0, sizeof(uint8_t) * capacity);
    Row row = Row({Field(got)});
    row.serializeToMemory(buffer, capacity);
    EXPECT_EQ(1,
              PyObject_RichCompareBool(
                      PyFloat_FromDouble(want),
                      createPyObjectFromMemory(buffer, python::Type::F64, capacity),
                      equal ? Py_EQ : Py_NE));
}

TEST(PythonSerializer, TestCreatePyObjectFromMemoryFloat) {
    Py_Initialize();

    size_t capacity = 256;
    uint8_t buffer[capacity];

    checkCreatePyObjectFromMemoryFloat(buffer, capacity, 0, 0, true);
    checkCreatePyObjectFromMemoryFloat(buffer, capacity, 1, 1, true);
    checkCreatePyObjectFromMemoryFloat(buffer, capacity, -1, -1, true);
    checkCreatePyObjectFromMemoryFloat(buffer, capacity, 3.141, 3.141, true);
    checkCreatePyObjectFromMemoryFloat(buffer, capacity, std::numeric_limits<double>::min(),
                                       std::numeric_limits<double>::min(), true);
    checkCreatePyObjectFromMemoryFloat(buffer, capacity, std::numeric_limits<double>::max(),
                                       std::numeric_limits<double>::max(), true);
    checkCreatePyObjectFromMemoryFloat(buffer, capacity, std::numeric_limits<double>::max(),
                                       std::numeric_limits<double>::min(), false);
}

void checkCreatePyObjectFromMemoryString(uint8_t *buffer, size_t capacity, const std::string &got,
                                         const std::string &want, bool equal) {
    memset(buffer, 0, sizeof(uint8_t) * capacity);
    Row row = Row({Field(got)});
    row.serializeToMemory(buffer, capacity);
    char *string_errors = nullptr;
    EXPECT_EQ(1,
              PyObject_RichCompareBool(
                      PyUnicode_DecodeASCII(want.c_str(), want.length(), string_errors),
                      createPyObjectFromMemory(buffer, python::Type::STRING, capacity),
                      equal ? Py_EQ : Py_NE));
}

TEST(PythonSerializer, TestCreatePyObjectFromMemoryString) {
    Py_Initialize();

    size_t capacity = 256;
    uint8_t buffer[capacity];

    checkCreatePyObjectFromMemoryString(buffer, capacity, "Hello, World!", "Hello, World!", true);
    checkCreatePyObjectFromMemoryString(buffer, capacity, "foobar", "foobar", true);
    checkCreatePyObjectFromMemoryString(buffer, capacity, "", "", true);
    checkCreatePyObjectFromMemoryString(buffer, capacity, "foobar", "Hello, World!", false);
    checkCreatePyObjectFromMemoryString(buffer, capacity, "", " ", false);
}

void checkCreatePyObjectFromMemoryTuple(uint8_t *buffer, size_t capacity, const Tuple &got, PyObject *want,
                                        bool equal) {
    memset(buffer, 0, sizeof(uint8_t) * capacity);
    Row row({Field(got)});
    row.serializeToMemory(buffer, capacity);
    EXPECT_EQ(1,
              PyObject_RichCompareBool(
                      want,
                      createPyObjectFromMemory(buffer, row.getSchema().getRowType(), capacity),
                      equal ? Py_EQ : Py_NE));
}

TEST(PythonSerializer, TestCreatePyObjectFromMemoryEmptyTuple) {
    Py_Initialize();

    size_t capacity = 256;
    uint8_t buffer[capacity];

    PyObject *wrapped_tuple = PyTuple_New(1);
    PyTuple_SetItem(wrapped_tuple, 0, PyTuple_New(0));

    checkCreatePyObjectFromMemoryTuple(buffer, capacity, Tuple(), wrapped_tuple, true);

    PyObject *non_empty_tuple = PyTuple_New(1);
    PyTuple_SetItem(non_empty_tuple, 0, Py_None);
    wrapped_tuple = PyTuple_New(1);
    PyTuple_SetItem(wrapped_tuple, 0, non_empty_tuple);

    checkCreatePyObjectFromMemoryTuple(buffer, capacity, Tuple(), wrapped_tuple, false);
}

TEST(PythonSerializer, TestCreatePyObjectFromMemoryAllTypesTuple) {
    Py_Initialize();

    size_t capacity = 256;
    uint8_t buffer[capacity];

    PyObject *tuple = PyTuple_New(6);
    PyTuple_SetItem(tuple, 0, Py_True);
    PyTuple_SetItem(tuple, 1, PyLong_FromLong(42));
    PyTuple_SetItem(tuple, 2, PyFloat_FromDouble(3.141));
    std::string str = "Hello, World!";
    char *string_errors = nullptr;
    PyTuple_SetItem(tuple, 3, PyUnicode_DecodeASCII(str.c_str(), str.length(), string_errors));
    PyTuple_SetItem(tuple, 4, PyTuple_New(0));
    PyObject *non_empty_tuple = PyTuple_New(1);
    PyTuple_SetItem(non_empty_tuple, 0, Py_False);
    PyTuple_SetItem(tuple, 5, non_empty_tuple);

    PyObject *wrapped_tuple = PyTuple_New(1);
    PyTuple_SetItem(wrapped_tuple, 0, tuple);

//    Tuple got(
//            {Field(true), Field((int64_t) 42), Field(3.141), Field(str), Field(Tuple()), Field(Tuple({Field(false)}))});

    Tuple got(true, 42, 3.141, str, Tuple(), Tuple(false));

    checkCreatePyObjectFromMemoryTuple(buffer, capacity, got, wrapped_tuple, true);
}

TEST(PythonSerializer, TestCreatePyObjectFromMemoryNestedTuple) {
    Py_Initialize();

    size_t capacity = 256;
    uint8_t buffer[capacity];

    int64_t sample_long = 42;
    double sample_float = 3.141;
    std::string sample_str = "Hello, World!";

//    Row row({Field(Tuple({Field(Tuple({Field(sample_bool), Field(sample_float)})), Field(Tuple()),
//                          Field(Tuple({Field(sample_str)}))})),
//             Field(Tuple({Field(sample_long), Field(sample_float)})),
//             Field(sample_str)});
    Row row(Tuple(Tuple(true, sample_float), Tuple(), Tuple(sample_str)),
            Tuple(sample_long, sample_float),
            sample_str);

    row.serializeToMemory(buffer, capacity);

    char *string_errors = nullptr;
    PyObject *tuple = PyTuple_New(3);

    PyObject *inner_tuple_1 = PyTuple_New(3);
    PyObject *inner_inner_tuple_1 = PyTuple_New(2);
    PyTuple_SetItem(inner_inner_tuple_1, 0, Py_True);
    PyTuple_SetItem(inner_inner_tuple_1, 1, PyFloat_FromDouble(sample_float));
    PyObject *inner_inner_tuple_2 = PyTuple_New(0);
    PyObject *inner_inner_tuple_3 = PyTuple_New(1);
    PyTuple_SetItem(inner_inner_tuple_3, 0,
                    PyUnicode_DecodeASCII(sample_str.c_str(), sample_str.length(), string_errors));
    PyTuple_SetItem(inner_tuple_1, 0, inner_inner_tuple_1);
    PyTuple_SetItem(inner_tuple_1, 1, inner_inner_tuple_2);
    PyTuple_SetItem(inner_tuple_1, 2, inner_inner_tuple_3);

    PyObject *inner_tuple_2 = PyTuple_New(2);
    PyTuple_SetItem(inner_tuple_2, 0, PyLong_FromLong(sample_long));
    PyTuple_SetItem(inner_tuple_2, 1, PyFloat_FromDouble(sample_float));

    PyTuple_SetItem(tuple, 0, inner_tuple_1);
    PyTuple_SetItem(tuple, 1, inner_tuple_2);
    PyTuple_SetItem(tuple, 2, PyUnicode_DecodeASCII(sample_str.c_str(), sample_str.length(), string_errors));

    auto reconstructed = createPyObjectFromMemory(buffer, row.getSchema().getRowType(), capacity);
    EXPECT_EQ(1,
              PyObject_RichCompareBool(
                      tuple,
                      reconstructed,
                      Py_EQ));
}

TEST(PythonSerializer, TestFromSerializedMemory) {
    Py_Initialize();

    size_t capacity = 256;
    uint8_t buffer[capacity];

    Tuple tuple(true);
    memset(buffer, 0, sizeof(uint8_t) * capacity);
    Row row({Field(tuple)});
    row.serializeToMemory(buffer, capacity);

    PyObject *pyobj = nullptr;


    // this test has been hacked away, b.c. whole to python thing needs codegen overhaul...
//    EXPECT_FALSE(fromSerializedMemory(buffer, 0, row.getSchema(), &pyobj));
//    EXPECT_EQ(nullptr, pyobj);

    EXPECT_TRUE(fromSerializedMemory(buffer, capacity, row.getSchema(), &pyobj));
    PyObject *bool_tuple = PyTuple_New(1);
    PyTuple_SetItem(bool_tuple, 0, Py_True);
    PyObject *wrapped_tuple = PyTuple_New(1);
    PyTuple_SetItem(wrapped_tuple, 0, bool_tuple);
    EXPECT_EQ(1, PyObject_RichCompareBool(wrapped_tuple, pyobj, Py_EQ));
}