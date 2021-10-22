//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Python.h>
#include <gtest/gtest.h>
#include <ContextOptions.h>
#include <vector>
#include <Utils.h>
#include <Context.h>
#include "TestUtils.h"
#include <CSVUtils.h>
#include <CSVStatistic.h>
#include <parser/Parser.h>
#include <TraceVisitor.h>

class TracerTest : public PyTest {};

TEST_F(TracerTest, SimpleTrace) {

    using namespace tuplex;
    using namespace std;

    // @TODO: Need to test here all the functions in
    // 1. Zillow workload ??
    // 2. Flight workload ??
    // 3. 311 workload ??
    // 4. log workload ??

    const std::string code = "lambda x: x + 1";

    auto ast = tuplex::parseToAST(code);

    python::lockGIL();
    TraceVisitor tv;
    tv.recordTrace(ast, PyLong_FromLong(10));
    python::unlockGIL();
}

PyObject* jsonToPyObject(const std::string& s) {
    auto obj = python::runAndGet("import json\nd = json.loads(' + s + ')", "d");
    return obj;
}

TEST_F(TracerTest, ZillowExtractBd) {
    auto code = "def extractBd(x):\n"
                       "    val = x['facts and features']\n"
                       "    max_idx = val.find(' bd')\n"
                       "    if max_idx < 0:\n"
                       "        max_idx = len(val)\n"
                       "    s = val[:max_idx]\n"
                       "\n"
                       "    # find comma before\n"
                       "    split_idx = s.rfind(',')\n"
                       "    if split_idx < 0:\n"
                       "        split_idx = 0\n"
                       "    else:\n"
                       "        split_idx += 2\n"
                       "    r = s[split_idx:]\n"
                       "    return int(r)";
    using namespace tuplex;
    using namespace std;

    // @TODO: Need to test here all the functions in
    // 1. Zillow workload ??
    // 2. Flight workload ??
    // 3. 311 workload ??
    // 4. log workload ??

    auto ast = tuplex::parseToAST(code);
    ASSERT_TRUE(ast);

    python::lockGIL();

//    jsonToPyObject("{\"facts and features\": \"3 bds , 1 ba , 1,560 sqft\"}");
//

    PyObject *in = PyDict_New();
    PyDict_SetItemString(in, "facts and features", python::PyString_FromString("3 bds , 1 ba , 1,560 sqft"));
//
//    auto obj = PyDict_GetItem(in, python::PyString_FromString("facts and features"));
//    PyObject_Print(obj, stdout, 0);
//    cout<<endl;

    // quick dict
    TraceVisitor tv;
    tv.recordTrace(ast, in);
    python::unlockGIL();

}

void traceAndValidateResult(const std::string& code, PyObject* arg) {
    using namespace std;
    using namespace tuplex;
    Py_XINCREF(arg);
    ASSERT_TRUE(arg);

    auto ast = tuplex::parseToAST(code);
    ASSERT_TRUE(ast);

    // use tracer
    TraceVisitor tv;
    tv.recordTrace(ast, arg);

    // fetch result
    auto res = tv.lastResult();
    ASSERT_TRUE(res);

    // for comparison, simply evaluate using compile func
    auto py_func = python::compileFunction(python::getMainModule(), code);
    ASSERT_TRUE(py_func && PyCallable_Check(py_func));

    PyObject* args = nullptr;
    if(PyTuple_Check(arg) && PyTuple_Size(arg) >= 1) {
        args = arg;
    } else {
        args = PyTuple_New(1);
        PyTuple_SetItem(args, 0, arg);
    }
    PyObject * ref = PyObject_Call(py_func, args, nullptr);
    ASSERT_TRUE(ref);

#ifndef NDEBUG
    cout<<"result:"<<endl;
    PyObject_Print(res, stdout, 0);
    cout<<endl;
    cout<<"reference:"<<endl;
    PyObject_Print(ref, stdout, 0);
    cout<<endl;
#endif

    EXPECT_TRUE(PyObject_RichCompare(ref, res, Py_EQ));
}

TEST_F(TracerTest, IsKeyword) {
    python::lockGIL();

    auto udf1 = "lambda x: x is None";
    PyObject* arg1 = Py_None;

    traceAndValidateResult(udf1, arg1);

    auto udf2 = "lambda x: x is not None";
    PyObject* arg2 = PyBool_FromLong(0);

    traceAndValidateResult(udf2, arg2);

    auto udf3 = "lambda x: x is not False";
    PyObject* arg3 = PyBool_FromLong(1);

    traceAndValidateResult(udf3, arg3);

    auto udf4 = "lambda x: x is True";
    PyObject* arg4 = PyBool_FromLong(1);

    traceAndValidateResult(udf4, arg4);

    python::unlockGIL();
}

TEST_F(TracerTest, UseCaseFunctions) {
    python::lockGIL();

    auto udf1 = "lambda x: x['OriginCityName'][:x['OriginCityName'].rfind(',')].strip()";
    PyObject* arg1 = PyDict_New();
    PyDict_SetItemString(arg1, "OriginCityName", python::PyString_FromString("Atlanta, GA"));

    traceAndValidateResult(udf1, arg1);

    auto udf2 = "lambda x: '{:02}:{:02}'.format(int(x / 100), x % 100)";
    PyObject* arg2 = PyLong_FromLong(1141);

    traceAndValidateResult(udf2, arg2);

    auto udf3 = "def extractDefunctYear(t):\n"
                     "  x = t['Description']\n"
                     "  desc = x[x.rfind('-')+1:x.rfind(')')].strip()\n"
                     "  return int(desc) if len(desc) > 0 else None";

    PyObject* arg3 = PyDict_New();
    PyDict_SetItemString(arg3, "Description", python::PyString_FromString("Comlux Aviation, AG (2006 - 2012)"));

    traceAndValidateResult(udf3, arg3);

    auto udf4 = "lambda x: '%05d' % int(x['postal_code'])";
    PyObject* arg4 = PyDict_New();
    PyDict_SetItemString(arg4, "postal_code", PyLong_FromLong(2906));

    traceAndValidateResult(udf4, arg4);

    python::unlockGIL();
}


// test here normal case/exception case compile for special null value opt case
//   auto fillInTimes_C = "def fillInTimesUDF(row):\n"
//                             "    ACTUAL_ELAPSED_TIME = row['ActualElapsedTime']\n"
//                             "    if row['DivReachedDest']:\n"
//                             "        if float(row['DivReachedDest']) > 0:\n"
//                             "            return float(row['DivActualElapsedTime'])\n"
//                             "        else:\n"
//                             "            return ACTUAL_ELAPSED_TIME\n"
//                             "    else:\n"
//                             "        return ACTUAL_ELAPSED_TIME";

// => ACTUAL_ELAPSED_TIME: f64
// => DIV_REACHED_DEST:  null (could be detected! then, branches need to be erased...)
// => DIV_ACTUAL_ELAPSED_TIME: null

// i.e. if inferSchema fails, then use TraceVisitor for sample. Then, add special node to jump out of compute. (some ASTNode)
// and throw internal exception.
// ==> this means, that slower code path is required. Two options here: 1.) can compile resolve code 2.) can't compile resolve code, need, interpreter...
// when creating the plan, need some way to figure out whether more conservative code needs to be created or not...
// @TODO.
// => use Neumann style code generation.

// TODO: test with function and ambiguous returns
// e.g. def f(x):
//         if x > 10:
//              return x
//         else:
//              return 'hello world'