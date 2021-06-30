//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include "TestUtils.h"
#include <Context.h>

using namespace tuplex;

TEST(PythonRow, Primitives) {
    PyInterpreterGuard g;

    PyObject* singleInt = python::rowToPython(Row(42), true);
    EXPECT_TRUE(PyLong_Check(singleInt));
    EXPECT_EQ(PyLong_AsLong(singleInt), 42);

    // dictionary
    std::string dict_str = std::string("{\"if123\":456.12,\"if789\":0.0}");
    PyObject *singleDict = python::rowToPython(Row({Field::from_str_data(dict_str, python::Type::makeDictionaryType(python::Type::I64, python::Type::F64))}), true);
    PyObject_Print(singleDict, stdout, 0);
    auto key1 = PyLong_FromLong(123);
    auto key2 = PyLong_FromLong(789);
    EXPECT_EQ(PyFloat_AsDouble(PyDict_GetItem(singleDict, key1)), 456.12);
    EXPECT_EQ(PyFloat_AsDouble(PyDict_GetItem(singleDict, key2)), 0.0);
}

TEST(PythonRow, Lists) {
    PyInterpreterGuard g;

    // empty list
    PyObject *emptyList = python::rowToPython(Row(List()), true);
    EXPECT_TRUE(PyList_Check(emptyList));
    EXPECT_EQ(PyList_Size(emptyList), 0);

    // one element list
    PyObject *singleElementList = python::rowToPython(Row(List(10)), true);
    EXPECT_TRUE(PyList_Check(singleElementList));
    EXPECT_EQ(PyList_Size(singleElementList), 1);
    auto obj = PyList_GetItem(singleElementList, 0);
    EXPECT_TRUE(PyLong_Check(obj));
    EXPECT_EQ(PyLong_AsLong(obj), 10);

    // multi-element list
    PyObject *multiElementList = python::rowToPython(Row(List(10.5, 11.5)), true);
    EXPECT_TRUE(PyList_Check(multiElementList));
    EXPECT_EQ(PyList_Size(multiElementList), 2);
    obj = PyList_GetItem(multiElementList, 0);
    EXPECT_TRUE(PyFloat_Check(obj));
    EXPECT_EQ(PyFloat_AsDouble(obj), 10.5);
    obj = PyList_GetItem(multiElementList, 1);
    EXPECT_TRUE(PyFloat_Check(obj));
    EXPECT_EQ(PyFloat_AsDouble(obj), 11.5);

    PyObject *multiElementListStr = python::rowToPython(Row(List("hello", "world", "!", "")), true);
    EXPECT_TRUE(PyList_Check(multiElementListStr));
    EXPECT_EQ(PyList_Size(multiElementListStr), 4);

    obj = PyList_GetItem(multiElementListStr, 0);
    EXPECT_TRUE(PyUnicode_Check(obj));
    EXPECT_EQ(std::string(PyUnicode_AsUTF8(obj)), std::string("hello"));

    obj = PyList_GetItem(multiElementListStr, 1);
    EXPECT_TRUE(PyUnicode_Check(obj));
    EXPECT_EQ(std::string(PyUnicode_AsUTF8(obj)), std::string("world"));

    obj = PyList_GetItem(multiElementListStr, 2);
    EXPECT_TRUE(PyUnicode_Check(obj));
    EXPECT_EQ(std::string(PyUnicode_AsUTF8(obj)), std::string("!"));

    obj = PyList_GetItem(multiElementListStr, 3);
    EXPECT_TRUE(PyUnicode_Check(obj));
    EXPECT_EQ(std::string(PyUnicode_AsUTF8(obj)), std::string(""));
}

TEST(PythonRow, Tuples) {
    PyInterpreterGuard g;

    // auto unpacking for empty tuple yields result
    PyObject* emptyTuple = python::rowToPython(Row(), true);
    EXPECT_TRUE(PyTuple_Check(emptyTuple));
    EXPECT_EQ(PyTuple_Size(emptyTuple), 0);

    emptyTuple = python::rowToPython(Row(), false);
    EXPECT_TRUE(PyTuple_Check(emptyTuple));
    EXPECT_EQ(PyTuple_Size(emptyTuple), 0);

    // auto unpack single tuple
    PyObject* singleElementTuple = python::rowToPython(Row(Tuple(42)), true);
    PyObject_Print(singleElementTuple, stdout, 0);
    EXPECT_TRUE(PyTuple_Check(singleElementTuple));
    EXPECT_EQ(PyTuple_Size(singleElementTuple), 1);
    PyObject* obj = PyTuple_GetItem(singleElementTuple, 0);
    EXPECT_TRUE(PyLong_Check(obj));
    EXPECT_EQ(PyLong_AsLong(obj), 42);


    // no-unpack, single tuple
    singleElementTuple = python::rowToPython(Row(Tuple(42)), false);
    PyObject_Print(singleElementTuple, stdout, 0);
    EXPECT_TRUE(PyTuple_Check(singleElementTuple));
    EXPECT_EQ(PyTuple_Size(singleElementTuple), 1);
    obj = PyTuple_GetItem(singleElementTuple, 0);
    EXPECT_TRUE(PyTuple_Check(obj));
    EXPECT_EQ(PyLong_AsLong(PyTuple_GetItem(obj, 0)), 42);

    PyObject* twoElementTuple = python::rowToPython(Row(Tuple(42, 84)), true);
    EXPECT_TRUE(PyTuple_Check(twoElementTuple));
    EXPECT_EQ(PyTuple_Size(twoElementTuple), 2);
    EXPECT_EQ(PyLong_AsLong(PyTuple_GetItem(twoElementTuple, 0)), 42);
    EXPECT_EQ(PyLong_AsLong(PyTuple_GetItem(twoElementTuple, 1)), 84);
}


/*!
 * helper function to be used in PythonFunc.Calls test
 * @param code code to compile
 * @param input input rows
 * @param ref ref, output should be integers.
 */
void testApplySimpleFunction(const std::string& code, const std::vector<Row>& input, const std::vector<Row>& ref) {
    PyObject* pFunc = python::compileFunction(python::getMainModule(), code);
    ASSERT_TRUE(pFunc);

    ASSERT_EQ(input.size(), ref.size());

    // go over
    for(int i = 0; i < input.size(); ++i) {
        ExceptionCode ec;
        PyObject *pRes = python::callFunction(pFunc, python::rowToPython(input[i]), ec);
        ASSERT_TRUE(pRes);
        EXPECT_EQ(ec, ExceptionCode::SUCCESS);

        // compare contents
        EXPECT_EQ(python::pythonToRow(pRes), ref[i]);
    }
}

TEST(PythonFunc, Calls) {
    PyInterpreterGuard g;

    // test different call types
    PyObject* pFunc=nullptr, *pRes=nullptr;

    // call on primitive should work
    ExceptionCode ec;
    pFunc = python::compileFunction(python::getMainModule(), "lambda x: x + 1");
    ASSERT_TRUE(pFunc);
    pRes = python::callFunction(pFunc, python::rowToPython(Row(12)), ec);
    ASSERT_TRUE(pRes);
    EXPECT_EQ(ec, ExceptionCode::SUCCESS);
    EXPECT_EQ(PyLong_AsLong(pRes), 13);

    // call on primitive with unpacking should fail
    // i.e. this syntax is not supported.
    pFunc = python::compileFunction(python::getMainModule(), "lambda x: x[0] + 1");
    pRes = python::callFunction(pFunc, python::rowToPython(Row(12)), ec);
    EXPECT_EQ(ec, ExceptionCode::TYPEERROR);


    // Following are some basic trafo examples, demonstrating

    // 1.
    // some basic examples
    // [1, 2, 3] -> lambda x: x -> [1, 2, 3]
    testApplySimpleFunction("lambda x: x", {Row(1), Row(2), Row(3)}, {Row(1), Row(2), Row(3)});

    // [(1, 2), (1, 3), (1, 4)] -> lambda x: x[0] -> [1, 1, 1]
    testApplySimpleFunction("lambda x: x[0]", {Row(1, 2), Row(1, 3), Row(1, 4)}, {Row(1), Row(1), Row(1)});

    // [(1, 2), (1, 3), (1, 4)] -> lambda x: x[1] -> [2, 3, 4]
    testApplySimpleFunction("lambda x: x[1]", {Row(1, 2), Row(1, 3), Row(1, 4)}, {Row(2), Row(3), Row(4)});

    // [(1, 2), (1, 3), (1, 4)] -> lambda a, b: a -> [1, 1, 1]
    testApplySimpleFunction("lambda a, b: a", {Row(1, 2), Row(1, 3), Row(1, 4)}, {Row(1), Row(1), Row(1)});

    // [(1, 2), (1, 3), (1, 4)] -> lambda a, b: b -> [2, 3, 4]
    testApplySimpleFunction("lambda a, b: b", {Row(1, 2), Row(1, 3), Row(1, 4)}, {Row(2), Row(3), Row(4)});


    // 2.
    // constant term
    testApplySimpleFunction("lambda: 11", {Row(1), Row(2), Row(3)}, {Row(11), Row(11), Row(11)});


    // 3.
    // Tuple objects
    // [(1, 2), (1, 3), (1, 4)] -> lambda x: x -> [(1, 2), (1, 3), (1, 4)]
    testApplySimpleFunction("lambda x: x", {Row(1, 2), Row(1, 3), Row(1, 4)}, {Row(1, 2), Row(1, 3), Row(1, 4)});

    // [(1, 2), (1, 3), (1, 4)] -> lambda x: (x) -> [(1, 2), (1, 3), (1, 4)]
    testApplySimpleFunction("lambda x: (x)", {Row(1, 2), Row(1, 3), Row(1, 4)}, {Row(1, 2), Row(1, 3), Row(1, 4)});

    // [(1, 2), (1, 3), (1, 4)] -> lambda x: (x,) -> [((1, 2),) ((1, 3),) ((1, 4),)]
    testApplySimpleFunction("lambda x: (x,)",
            {Row(1, 2), Row(1, 3), Row(1, 4)},
            {Row(Tuple(1, 2)), Row(Tuple(1, 3)), Row(Tuple(1, 4))});

    // [(1, 2), (1, 3), (1, 4)] -> lambda a, b: (a, b) -> [(1, 2), (1, 3), (1, 4)]
    testApplySimpleFunction("lambda a, b: (a, b)",
            {Row(1, 2), Row(1, 3), Row(1, 4)},
            {Row(1, 2), Row(1, 3), Row(1, 4)});

    // [1, 2, 3] -> lambda x: () -> [(), (), ()]
    testApplySimpleFunction("lambda x: ()",
            {Row(1, 2), Row(1, 3), Row(1, 4)},
            {Row(Tuple()), Row(Tuple()), Row(Tuple())});


    // 4. later: dictionary mode @Todo:

}

TEST(PythonFunc, TracebackLambdaCompile) {
    PyInterpreterGuard g;

    std::string code = "\n\nlambda x: (x\n,10 / x)"; // note the \n at the beginning and within the lambda
    // to make the test more interesting. => this can also happen as some weird func extract process, so keep it here.

    auto pFunc = python::compileFunction(python::getMainModule(), code);

    auto pcr = python::callFunctionEx(pFunc, python::rowToPython((Row(0))));

    EXPECT_EQ(pcr.res, nullptr);
    EXPECT_EQ(pcr.exceptionClass, "ZeroDivisionError");
    EXPECT_EQ(pcr.exceptionLineNo, 4);
    EXPECT_EQ(pcr.functionFirstLineNo, 3);
    EXPECT_EQ(pcr.functionName, "<lambda>");
}

TEST(PythonFunc, TracebackLambdaPickled) {
    PyInterpreterGuard g;

    std::string code = "\n\nlambda x: (x\n,10 / x)"; // note the \n at the beginning and within the lambda
    // to make the test more interesting.
    auto pickled_code = python::serializeFunction(python::getMainModule(), code);

    PyObject* pFunc = python::deserializePickledFunction(python::getMainModule(),
            pickled_code.c_str(),
            pickled_code.length());

    auto pcr = python::callFunctionEx(pFunc, python::rowToPython((Row(0))));

    EXPECT_EQ(pcr.res, nullptr);
    EXPECT_EQ(pcr.exceptionClass, "ZeroDivisionError");
    EXPECT_EQ(pcr.exceptionLineNo, 4);
    EXPECT_EQ(pcr.functionFirstLineNo, 3);
    EXPECT_EQ(pcr.functionName, "<lambda>");
}



//TEST(PythonFunc, Traceback) {
//    python::initInterpreter();
//
//    // this example should yield a line-no of 2 (correct with code above)
////    std::string code = "lambda x: (x\n,10 / x)";
////    // produce traceback for UDF?
////    PyObject* pFunc = python::compileFunction(python::getMainModule(), code);
////
////    ExceptionCode ec;
////    callWithTraceback(pFunc, python::rowToPython(Row(0)));
//
//
//
//     std::string code = "\n\nlambda x: (x\n,10 / x)"; // this gives start no 2.
//    // std::string code = "\n\nlambda x: (x\n,10 / x)"; // this gives start no 4.
////    std::string code = "def f(x):\n"
////                       "    return x"; //
//
//    // now cloudpickle. Will this destroy results? ==> required for remote execution!!!
//    std::string pickled_code = python::serializeFunction(python::getMainModule(), code);
//
//    //std::cout<<pickled_code<<std::endl;
//
//    // produce traceback for UDF?
//    PyObject* pFunc = python::deserializePickledFunction(python::getMainModule(),
//            pickled_code.c_str(),
//            pickled_code.length());
//
//    ExceptionCode ec;
//    auto pcr = callWithTraceback(pFunc, python::rowToPython(Row(0)));
//
//
//    Py_XDECREF(pFunc);
//    python::closeInterpreter();
//}