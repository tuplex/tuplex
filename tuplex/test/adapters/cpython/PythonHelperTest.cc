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
#include "PythonHelpers.h"

using namespace tuplex;
using namespace tuplex::cpython;
using namespace python;

class PythonHelperTest : public ::testing::Test {
protected:
    void SetUp() override {
        // init python interpreter
        python::initInterpreter();
    }

    void TearDown() override {
        // close python interpreter
        python::closeInterpreter();
    }
};

TEST_F(PythonHelperTest, pythonToTuplexSimpleTypes) {
    auto x1 = PyLong_FromLong(1234);
    auto x2 = PyFloat_FromDouble(3.14159);
    auto x3 = PyBool_FromLong(1);
    auto x4 = PyString_FromString("Hello world!");
    auto x5 = PyTuple_New(0);
    // @Todo: add None type + some python object


    Row r1 = pythonToRow(x1);
    Row r2 = pythonToRow(x2);
    Row r3 = pythonToRow(x3);
    Row r4 = pythonToRow(x4);
    Row r5 = pythonToRow(x5);

    EXPECT_EQ(r1, Row(1234));
    EXPECT_EQ(r2, Row(3.14159));
    EXPECT_EQ(r3, Row(true));
    EXPECT_EQ(r4, Row("Hello world!"));
    EXPECT_EQ(r5, Row(Tuple()));
}

TEST_F(PythonHelperTest, TUPLEXToPythonSimpleTypes) {
    auto ref1 = PyLong_FromLong(1234);
    auto ref2 = PyFloat_FromDouble(3.14159);
    auto ref3 = PyBool_FromLong(1);
    auto ref4 = PyString_FromString("Hello world!");
    auto ref5 = PyTuple_New(0);


    // activate auto-unpacking
    auto x1 = rowToPython(Row(1234), true);
    auto x2 = rowToPython(Row(3.14159), true);
    auto x3 = rowToPython(Row(true), true);
    auto x4 = rowToPython(Row("Hello world!"), true);
    auto x5 = rowToPython(Row(Tuple()), true);

    // python compare
    // cf. https://docs.python.org/3/c-api/object.html
    // -1 means errors, 0 is false, 1 is true
    EXPECT_EQ(PyObject_RichCompareBool(ref1, x1, Py_EQ), 1);
    EXPECT_EQ(PyObject_RichCompareBool(ref2, x2, Py_EQ), 1);
    EXPECT_EQ(PyObject_RichCompareBool(ref3, x3, Py_EQ), 1);
    EXPECT_EQ(PyObject_RichCompareBool(ref4, x4, Py_EQ), 1);
    EXPECT_EQ(PyObject_RichCompareBool(ref5, x5, Py_EQ), 1);
}


TEST_F(PythonHelperTest, pythonToTuplexNestedTupleI) {


    // test 1: ((20, "Hello"), (), 3)
    auto l11 = PyTuple_New(2);
    PyTuple_SET_ITEM(l11, 0, PyLong_FromLong(20));
    PyTuple_SET_ITEM(l11, 1, PyString_FromString("Hello"));
    auto l12 = PyTuple_New(0);

    auto x1 = PyTuple_New(3);
    PyTuple_SET_ITEM(x1, 0, l11);
    PyTuple_SET_ITEM(x1, 1, l12);
    PyTuple_SET_ITEM(x1, 2, PyLong_FromLong(3));


    Row r1 = pythonToRow(x1);
    Row ref1(Tuple(20, "Hello"), Tuple(), 3);
    EXPECT_EQ(r1, ref1);
}

TEST_F(PythonHelperTest, pythonToTuplexNestedTupleII) {

    // test 2: ((((30,),),),)
    auto l2111 = PyTuple_New(1);
    PyTuple_SET_ITEM(l2111, 0, PyLong_FromLong(30));
    auto l211 = PyTuple_New(1);
    PyTuple_SET_ITEM(l211, 0, l2111);
    auto l21 = PyTuple_New(1);
    PyTuple_SET_ITEM(l21, 0, l211);
    auto x2 = PyTuple_New(1);
    PyTuple_SET_ITEM(x2, 0, l21);

    Row r2 = pythonToRow(x2);
    Row ref2(Tuple(Field(Tuple(Field(Tuple(30))))));

    EXPECT_EQ(r2, ref2);
}

TEST_F(PythonHelperTest, typeMap) {

    // primitive types
    EXPECT_EQ(python::Type::BOOLEAN, python::mapPythonClassToTuplexType(Py_True));
    EXPECT_EQ(python::Type::BOOLEAN, python::mapPythonClassToTuplexType(Py_False));

    EXPECT_EQ(python::Type::I64, python::mapPythonClassToTuplexType(PyLong_FromLong(0)));
    EXPECT_EQ(python::Type::I64, python::mapPythonClassToTuplexType(PyLong_FromLong(-42)));
    EXPECT_EQ(python::Type::I64, python::mapPythonClassToTuplexType(PyLong_FromLong(1234560)));

    EXPECT_EQ(python::Type::F64, python::mapPythonClassToTuplexType(PyFloat_FromDouble(0.0)));
    EXPECT_EQ(python::Type::F64, python::mapPythonClassToTuplexType(PyFloat_FromDouble(-1.0)));
    EXPECT_EQ(python::Type::F64, python::mapPythonClassToTuplexType(PyFloat_FromDouble(3.123456789)));

    EXPECT_EQ(python::Type::STRING, python::mapPythonClassToTuplexType(python::PyString_FromString("")));
    EXPECT_EQ(python::Type::STRING, python::mapPythonClassToTuplexType(python::PyString_FromString("hello world")));

    // compound types
    PyObject* c1 = PyTuple_New(0);
    EXPECT_EQ(python::Type::EMPTYTUPLE, python::mapPythonClassToTuplexType(c1));

    PyObject* c2 = PyTuple_New(5);
    PyTuple_SetItem(c2, 0, Py_True);
    PyTuple_SetItem(c2, 1, PyLong_FromLong(0));
    PyTuple_SetItem(c2, 2, PyFloat_FromDouble(-1.0));
    PyTuple_SetItem(c2, 3, c1);
    PyTuple_SetItem(c2, 4, python::PyString_FromString("abc"));
    auto t2 = python::Type::makeTupleType({python::Type::BOOLEAN, python::Type::I64, python::Type::F64, python::Type::EMPTYTUPLE, python::Type::STRING});
    EXPECT_EQ(t2, python::mapPythonClassToTuplexType(c2));

    // dict (keytype, valuetype)
    PyObject* c3 = PyDict_New();
    PyDict_SetItemString(c3, "x", Py_True);
    PyDict_SetItemString(c3, "y", Py_False);
    auto t3 = python::Type::makeDictionaryType(python::Type::STRING, python::Type::BOOLEAN);
    EXPECT_EQ(t3, python::mapPythonClassToTuplexType(c3));

    // @TODO: to represent dicts as struct type, there should be also specific type -> generic type
    PyObject* c4 = PyDict_New();
    PyDict_SetItemString(c3, "x", Py_True);
    PyDict_SetItemString(c3, "y", PyFloat_FromDouble(3.141));

    // EXPECT_EQ(...) some struct type here...

    // dict => generic type, i.e. mixed key/val types.
    PyObject *c5 = PyDict_New();
    PyDict_SetItem(c5, Py_True, Py_False);
    PyDict_SetItem(c5, PyLong_FromLong(42), python::PyString_FromString("hello world"));
    PyDict_SetItemString(c5, "test", PyLong_FromLong(42));
    EXPECT_EQ(python::Type::GENERICDICT, python::mapPythonClassToTuplexType(c5));
}

TEST_F(PythonHelperTest, PythonConversion) {
    using namespace std;
    using namespace tuplex;

    vector<Row> rows;
    vector<Row> ref;
    int N = 1000;
    for(int i = 0; i < N; ++i) {
        rows.emplace_back(Row(2, 3, "test"));
        ref.emplace_back(Row(5, 4));
    }

    // convert to python objects
    vector<PyObject*> out;
    PyObject* func = python::compileFunction(python::getMainModule(), "lambda a, b, c: (a + b, len(c))");
    ASSERT_TRUE(func);

    for(auto row : rows) {
        PyObject* arg = python::rowToPython(row);
        ExceptionCode ec;
        PyObject* res = python::callFunction(func, arg, ec);
        EXPECT_EQ(ec, ExceptionCode::SUCCESS);
        out.emplace_back(res);
    }
}


TEST_F(PythonHelperTest, SimpleLambdaFromString) {

    auto func = python::compileFunction(python::getMainModule(), "lambda x: 0");

    auto pcr = python::callFunctionEx(func, python::rowToPython(Row(20)));

    EXPECT_EQ(pcr.exceptionCode, ExceptionCode::SUCCESS);
    EXPECT_EQ(0, PyLong_AsLong(pcr.res));
}

TEST_F(PythonHelperTest, SimpleLambdaFromStringII) {

    auto func = python::compileFunction(python::getMainModule(), "lambda x: (x, x * x)");

    auto pcr = python::callFunctionEx(func, python::rowToPython(Row(20)));

    EXPECT_EQ(pcr.exceptionCode, ExceptionCode::SUCCESS);
    Row res = python::pythonToRow(pcr.res);

    EXPECT_EQ(res, Row(20, 400));
}


TEST_F(PythonHelperTest, MultithreadedUDFCalls) {
    // this test checks whether locking & Co works for UDF calls

    using namespace std;
    using namespace tuplex;

    // test config here...
    int N = 10000;
    int num_threads = 4;

    PyObject* TUPLEX = python::compileFunction(python::getMainModule(), "lambda x: (x, x * x)");

    vector<Row> rows;
    for(int i = 0; i < N; ++i) {
        rows.emplace_back(Row(i));
    }

    python::unlockGIL();

    cout<<"Running lambda via single-threaded python..."<<endl;
    vector<Row> ref;
    for(int i = 0; i < N; ++i) {

        python::lockGIL();
        PyObject* row = python::rowToPython(rows[i]);

        auto pcr = python::callFunctionEx(TUPLEX, row);

        ref.push_back(python::pythonToRow(pcr.res));
        python::unlockGIL();
    }
    cout<<"single threaded done!"<<endl;


    // MULTITHREADED VERSION
    // start threads
    vector<std::thread> threads;

    struct ThreadInfo {
        vector<Row> output;
        std::string name;
    };

    vector<ThreadInfo*> infos;
    cout<<"Running lambda via multi-threaded (lock-based) python..."<<endl;
    for(int j = 0; j < num_threads; ++j) {

        auto info = new ThreadInfo();
        info->name = "thread" + std::to_string(j);
        infos.push_back(info);

        threads.emplace_back(std::thread([j,N,num_threads, &rows, &TUPLEX](ThreadInfo* ti) {

            for(int i = j * (N / num_threads); i < (j+1) * (N / num_threads); ++i) {

                python::lockGIL();
                PyObject* row = python::rowToPython(rows[i]);

                auto pcr = python::callFunctionEx(TUPLEX, row); // this produces an error...

                auto out_row = python::pythonToRow(pcr.res);
                ti->output.push_back(out_row);
                python::unlockGIL();
            }

        }, info));
    }

    for(int j = 0; j < num_threads; ++j)
        threads[j].join();

    cout<<"multi-threaded done!"<<endl;
    python::lockGIL();

    for(int j = 0; j < num_threads; ++j)
        delete infos[j];
}

TEST_F(PythonHelperTest, SchemaDecoding) {
    using namespace std;
    using namespace tuplex;

    auto code = "t0 = None\n"
                "t1 = bool\n"
                "t2 = int\n"
                "t3 = float\n"
                "t4 = str\n"
                "t5 = (int)\n"
                "t6 = (int,)\n"
                "t7 = (int, bool)\n"
                "t8 = [str]\n"
                "t9 = [int, float]\n"
                "t10 = ((int,), str)\n"
                "t11 = {}\n"
                "t12 = tuple\n"
                "t13 = dict\n"
                "\n"
                "import typing\n"
                "t14 = typing.Dict[str, float]\n"
                "t15 = typing.Dict[str, str]\n"
                "t16 = typing.Tuple[float]\n"
                "t17 = typing.Tuple[float, str]\n"
                "t18 = typing.Tuple[float, typing.Tuple[str, str], int]\n"
                "t19 = typing.Any\n"
                "from typing import *\n"
                "t20 = Tuple[Tuple[str, int], Dict[str, str]]";

#warning "add here support List typing objects in the future! I.e. list and List[float] or so need to be supported"

    // typing module...
    auto main_mod = python::getMainModule();
    auto main_dict = PyModule_GetDict(main_mod);

    PyRun_String(code, Py_file_input, main_dict, main_dict);
    ASSERT_FALSE(PyErr_Occurred());

    // go over symbols and check types
    vector<python::Type> ref{python::Type::UNKNOWN,
                             python::Type::BOOLEAN,
                             python::Type::I64,
                             python::Type::F64,
                             python::Type::STRING,
                             python::Type::I64,
                             python::Type::makeTupleType({python::Type::I64}),
                             python::Type::makeTupleType({python::Type::I64, python::Type::BOOLEAN}),
                             python::Type::makeTupleType({python::Type::STRING}),
                             python::Type::makeTupleType({python::Type::I64, python::Type::F64}),
                             python::Type::makeTupleType({python::Type::makeTupleType({python::Type::I64}), python::Type::STRING}),
                             python::Type::EMPTYDICT,
                             python::Type::GENERICTUPLE,
                             python::Type::GENERICDICT,
                             python::Type::makeDictionaryType(python::Type::STRING, python::Type::F64),
                             python::Type::makeDictionaryType(python::Type::STRING, python::Type::STRING),
                             python::Type::makeTupleType({python::Type::F64}),
                             python::Type::makeTupleType({python::Type::F64, python::Type::STRING}),
                             python::Type::makeTupleType({python::Type::F64, python::Type::makeTupleType({python::Type::STRING, python::Type::STRING}), python::Type::I64}),
                             python::Type::PYOBJECT,
                             python::Type::makeTupleType({ python::Type::makeTupleType({python::Type::STRING, python::Type::I64}), python::Type::makeDictionaryType(python::Type::STRING, python::Type::STRING) })};

    // fetch object from dict
    cout<<endl;
    for(int i = 0; i < ref.size(); ++i) {
        std::string key_str = ("t" + std::to_string(i));
        auto key = key_str.c_str();
        PyObject *obj = PyDict_GetItemString(main_dict, key);
        ASSERT_TRUE(obj);
        auto type = decodePythonSchema(obj);
        cout<<(i+1)<<"/"<<ref.size()<<" actual: "<<type.desc()<<" expected: "<<ref[i].desc()<<endl;
        EXPECT_EQ(type, ref[i]);
    }
}

TEST_F(PythonHelperTest, FunctionGlobals) {
    using namespace std;

    // typing module...
    auto main_mod = python::getMainModule();
    auto main_dict = PyModule_GetDict(main_mod);

    string code = "GLOBAL_VARIABLE = 300\n"
                  "\n"
                  "def func(x):\n"
                  "    return x + GLOBAL_VARIABLE";

    PyObject* pyFunc = python::runAndGet(code, "func");

    // fetch globals, locals
    cout<<endl;
    cout<<"globals: "<<endl;
    PyObject_Print(PyFunction_GetGlobals(pyFunc), stdout, 0);

    // convert globals object to lookup dictionary
    unordered_map<string, tuple<string, python::Type>> globals;
    auto pyGlobals = PyFunction_GetGlobals(pyFunc);
    // iterate over dictionary
    PyObject *key = nullptr, *val = nullptr;
    Py_ssize_t pos = 0; // must be initialized to 0 to start iteration, however internal iterator variable. Don't use semantically.
    while(PyDict_Next(pyGlobals, &pos, &key, &val)) {
        auto curKeyType = mapPythonClassToTuplexType(key);
        auto curValType = mapPythonClassToTuplexType(val);
        assert(curKeyType == python::Type::STRING);

        // check if value can be dealt with
        auto skey = python::PyString_AsString(key);
        globals[skey] = make_tuple(python::PyString_AsString(val), curValType);
    }

    // iterate over globals
    for(auto item : globals)
        cout<<item.first<<" ["<<std::get<1>(item.second).desc()<<"]: "<<std::get<0>(item.second)<<endl;

    // simpyl get function globals via f.__globals__

    cout<<endl;


    ASSERT_TRUE(PyCallable_Check(pyFunc));
    PyObject* pyArgs = PyTuple_Pack(1, PyLong_FromLong(10));
    PyObject* pyRes = PyObject_Call(pyFunc, pyArgs, nullptr);
    EXPECT_EQ(PyLong_AS_LONG(pyRes), 310);
}

TEST_F(PythonHelperTest, SchemaEncoding) {
    using namespace std;

    // primitive types
    auto x0 = python::encodePythonSchema(python::Type::NULLVALUE);
    EXPECT_EQ(python::PyString_AsString(x0), "<class 'NoneType'>");
    auto x1 = python::encodePythonSchema(python::Type::BOOLEAN);
    EXPECT_EQ(python::PyString_AsString(x1), "<class 'bool'>");
    auto x2 = python::encodePythonSchema(python::Type::I64);
    EXPECT_EQ(python::PyString_AsString(x2), "<class 'int'>");
    auto x3 = python::encodePythonSchema(python::Type::F64);
    EXPECT_EQ(python::PyString_AsString(x3), "<class 'float'>");
    auto x4 = python::encodePythonSchema(python::Type::STRING);
    EXPECT_EQ(python::PyString_AsString(x4), "<class 'str'>");

    auto y1 = python::encodePythonSchema(python::Type::makeOptionType(python::Type::I64));
    // there was a correction from Python 3.8 -> 3.9
#if (PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION <= 8)
    EXPECT_EQ(python::PyString_AsString(y1), "typing.Union[int, NoneType]");
#else
    EXPECT_EQ(python::PyString_AsString(y1), "typing.Optional[int]");
#endif

    auto y2 = python::encodePythonSchema(python::Type::makeDictionaryType(python::Type::I64, python::Type::STRING));
    EXPECT_EQ(python::PyString_AsString(y2), "typing.Dict[int, str]");

    auto z3 = python::encodePythonSchema(python::Type::makeTupleType({python::Type::I64,
                                                                      python::Type::EMPTYTUPLE,
                                                                      python::Type::F64}));
    EXPECT_EQ(python::PyString_AsString(z3), "(<class 'int'>, (), <class 'float'>)");

    auto w0 = python::encodePythonSchema(python::Type::makeFunctionType(python::Type::I64, python::Type::STRING));
    EXPECT_EQ(python::PyString_AsString(w0), "typing.Callable[[int], str]");
}

TEST_F(PythonHelperTest, FindStdlib) {

    EXPECT_NE(python::python_version(), "");

    auto loc = python::find_stdlib_location();
    std::cout<<"Found python stdlib location to be in: "<<loc<<std::endl;
    EXPECT_NE(loc, "");
}