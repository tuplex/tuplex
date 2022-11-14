//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/execution/HybridHashTable.h>
#include <physical/codegen/PythonPipelineBuilder.h>
#include <bucket.h>
#include "TestUtils.h"


// # normalize result according to Tuplex's typing rules.
//# ==> i.e. make dictionary syntax only fancy access pattern, but not the normal mode of operation!
//# ==> tuples are much faster and also reflect that there should be order!
//def propagate_result(row):
//
//    # recursive expansion of Row objects potentially present in data.
//    def expand_row(x):
//        if hasattr(type(x), '__iter__') and not isinstance(x, str):
//            if isinstance(x, tuple):
//                return tuple([expand_row(el) for el in x])
//            elif isinstance(x, list):
//                return [expand_row(el) for el in x]
//            elif isinstance(x, dict):
//                return {expand_row(key) : expand_row(val) for key, val in x.items()}
//            else:
//                raise TypeError("custom sequence type used, can't convert to data representation")
//        return x.data if isinstance(x, Row) else x
//
//    # is it a row object?
//    # => convert to tuple!
//    row = expand_row(row)
//
//    if not isinstance(row, tuple):
//        return (row,)
//    else:
//        if len(row) == 0:
//            return ((),) # special case, empty tuple
//        return row

PyObject *compile_pipeline(const std::string& code) {
// run in interpreter
    auto main_mod = python::getMainModule();
    auto main_dict = PyModule_GetDict(main_mod);

    PyRun_String(code.c_str(), Py_file_input, main_dict, main_dict);

    if(PyErr_Occurred()) {
        PyErr_Print();
    std::cout<<std::endl; }

    assert(!PyErr_Occurred());

// fetch function
    PyObject *pipFunction = PyDict_GetItemString(main_dict, "pipeline");
    assert(pipFunction);
    assert(PyCallable_Check(pipFunction));
    return pipFunction;
}


std::vector<tuplex::Row> call_pipeline(const tuplex::Row& row, PyObject* func) {

    auto args = PyTuple_New(1);
    PyTuple_SetItem(args, 0, python::rowToPython(row));
    auto pcr = python::callFunctionEx(func, args);

    assert(pcr.res);
    assert(PyDict_Check(pcr.res));
    assert(PyDict_GetItemString(pcr.res, "outputRows"));
    PyObject_Print(pcr.res, stdout, 0);
    std::cout<<std::endl;

    auto res = PyDict_GetItemString(pcr.res, "outputRows");
    assert(PyList_Check(res));
    std::vector<tuplex::Row> v;
    for(int i = 0; i < PyList_Size(res); ++i) {
        v.emplace_back(python::pythonToRow(PyList_GetItem(res, i)));
    }

    return v;
}

std::vector<tuplex::Row> call_pipeline(const std::string& line,
                                       PyObject* func,
                                       const std::vector<PyObject*>& hashmaps) {

    auto args = PyTuple_New(1 + hashmaps.size());
    PyTuple_SET_ITEM(args, 0, python::PyString_FromString(line.c_str()));
    for(int i = 0; i < hashmaps.size(); ++i)
        PyTuple_SET_ITEM(args, 1 + i, hashmaps[i]);
    auto pcr = python::callFunctionEx(func, args);

    assert(pcr.res);
    assert(PyDict_Check(pcr.res));
    assert(PyDict_GetItemString(pcr.res, "outputRows"));
    PyObject_Print(pcr.res, stdout, 0);
    std::cout<<std::endl;

    auto res = PyDict_GetItemString(pcr.res, "outputRows");
    assert(PyList_Check(res));
    std::vector<tuplex::Row> v;
    for(int i = 0; i < PyList_Size(res); ++i) {
        v.emplace_back(python::pythonToRow(PyList_GetItem(res, i)));
    }

    return v;
}


TEST(PythonPipeline, SimplePipeline) {
    using namespace tuplex;

    const std::string pipelineFuncName = "pipeline";

    python::initInterpreter();
    python::unlockGIL();

    PythonPipelineBuilder ppb(pipelineFuncName);

    ppb.csvInput(1001, {"a", "b"});
    ppb.mapOperation(1002, UDF("lambda x: (x['a'], x['b'], x['a'] + x['b'])"));
    ppb.filterOperation(1003, UDF("lambda x: x[2] < 10"));
    ppb.tuplexOutput(1004, python::Type::makeTupleType({python::Type::I64, python::Type::I64, python::Type::I64}));


    auto pycode = ppb.getCode();

    std::cout<<std::endl;
    std::cout<<"TEST CODE is:\n"<<std::endl;
    std::cout<<core::withLineNumbers(pycode)<<std::endl;

    python::lockGIL();


    // run code
    PyRun_SimpleString(pycode.c_str());


    // get object from main dict
    auto mod = python::getMainModule();
    auto pipelineFunc = PyObject_GetAttrString(mod, pipelineFuncName.c_str());
    ASSERT_TRUE(PyCallable_Check(pipelineFunc));


    // define here row object
    Row row(10, 20);
    // call function and fetch result
    auto args = PyTuple_New(1);
    PyTuple_SetItem(args, 0, python::rowToPython(row));
    auto resObj = PyObject_Call(pipelineFunc, args, nullptr);

    PyObject_Print(resObj, stdout, 0);
    std::cout<<std::endl;

#warning "no tests here in interest of time wrt paper"

    python::closeInterpreter();
}

// Test here whether generated python code understands auto dict unpacking
TEST(PythonPipeline, DictUnpacking) {
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    PythonPipelineBuilder ppb("pipeline");

    ppb.csvInput(1001, {"a", "b"});
    ppb.mapColumn(1002, "a", UDF("lambda x: x * x"));
    ppb.withColumn(1002, "b", UDF("lambda x: x['b'] + 1")); // not necessary, overwrite!
    ppb.withColumn(1003, "c", UDF("lambda x: x['a'] + x['b']"));
    ppb.csvOutput();

    auto code = ppb.getCode();

    std::cout<<code<<std::endl;

    python::lockGIL();

    auto pipFunction = compile_pipeline(code);

    // call function with tuple as arg
    auto pcr = python::callFunctionEx(pipFunction, python::rowToPython(Row("10, 20")));

    ASSERT_TRUE(pcr.res);
    ASSERT_TRUE(PyDict_Check(pcr.res));
    ASSERT_TRUE(PyDict_GetItemString(pcr.res, "outputRows"));
    PyObject_Print(pcr.res, stdout, 0);
    std::cout<<std::endl;

    // check output
    auto ref = "['" + std::to_string(10 * 10) + "," + std::to_string(21) + "," + std::to_string(10 * 10 + 21) + "\\n']";
    auto res = python::PyString_AsString(PyDict_GetItemString(pcr.res, "outputRows"));
    EXPECT_EQ(res, ref);

    // further, check output columns, which should be a, b, c
    auto outCols = PyDict_GetItemString(pcr.res, "outputColumns");
    ASSERT_TRUE(PyTuple_Check(outCols));
    ASSERT_EQ(PyTuple_Size(outCols), 3);
    EXPECT_EQ(python::PyString_AsString(PyTuple_GetItem(outCols, 0)), "a");
    EXPECT_EQ(python::PyString_AsString(PyTuple_GetItem(outCols, 1)), "b");
    EXPECT_EQ(python::PyString_AsString(PyTuple_GetItem(outCols, 2)), "c");

    python::closeInterpreter();
}

TEST(PythonPipeline, Resolvers) {
    // todo:
    // add resolvers for:
    // 1.) CSV parsing 2.) any other functions...

     using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    PythonPipelineBuilder ppb("pipeline");

    ppb.objInput(1001, {"a", "b", "c", "d"});
    ppb.mapColumn(1002, "a", UDF("lambda x: 10 / x"));
    ppb.resolve(2002, ExceptionCode::VALUEERROR, UDF("lambda x: 0"));
    ppb.resolve(2000, ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: 42"));
    ppb.ignore(2003, ExceptionCode::INDEXERROR);
    ppb.withColumn(1002, "b", UDF("lambda x: x['b'] + 1")); // not necessary, overwrite!
    ppb.withColumn(1003, "c", UDF("lambda x: x['a'] + x['b']"));
    ppb.pythonOutput();

    // @TODO: could have here output with type check + resolver! --> add this later as soon as something like ORC
    // or so is used.

    std::cout<<std::endl;
    std::cout<<"TEST CODE is:\n"<<std::endl;
    std::cout<<core::withLineNumbers(ppb.getCode())<<std::endl;

    python::lockGIL();

    auto pipFunction = compile_pipeline(ppb.getCode());

    // call with fake row input s.t. that ZeroDivResolver gets invoked...
    auto res = call_pipeline(Row(0, 1, 2, 3), pipFunction);
    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], Row(42, 2, 44, 3));

    // no resolvers, just regular path
    res = call_pipeline(Row(10, 1, 2, 3), pipFunction);
    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(res[0], Row(1.0, 2, 3.0, 3));
    python::closeInterpreter();
}

std::string toTuplexBytes(const python::Type& type, const std::string& var_name) {

    // simple types
    if(type == python::Type::BOOLEAN) {
        return "(int(" + var_name + ")).to_bytes(8,byteorder='"
        + ( endianness() == Endianess::ENDIAN_BIG ? "big" : "little")
        + "',signed=True)";
    } else if(type == python::Type::I64) {
        return "(" + var_name + ").to_bytes(8,byteorder='"
               + ( endianness() == Endianess::ENDIAN_BIG ? "big" : "little")
               + "',signed=True)";
    } else if(type == python::Type::F64) {
        return "(" + var_name + ").to_bytes(8,byteorder='"
               + ( endianness() == Endianess::ENDIAN_BIG ? "big" : "little")
               + "',signed=True)";
    } else if(type == python::Type::STRING) {
        return var_name + ".encode('utf-8')";
    } else throw std::runtime_error("not supported type");
}

TEST(PythonPipeline, SerializeToTuplexBinaryFormat) {
    using namespace tuplex;
    using namespace std;

    cout<<toTuplexBytes(python::Type::I64, "x")<<endl;

    using namespace tuplex;

    const std::string pipelineFuncName = "pipeline";

    python::initInterpreter();
    python::unlockGIL();

    PythonPipelineBuilder ppb(pipelineFuncName);

    ppb.csvInput(1001, {"a", "b"});
    ppb.mapOperation(1002, UDF("lambda x: (x['a'], x['b'], x['a'] + x['b'])"));
    ppb.filterOperation(1003, UDF("lambda x: x[2] < 10"));
    ppb.tuplexOutput(1004, python::Type::makeTupleType({python::Type::I64, python::Type::I64, python::Type::I64}));


    auto pycode = ppb.getCode();

    std::cout<<std::endl;
    std::cout<<"TEST CODE is:\n"<<std::endl;
    std::cout<<pycode<<std::endl;

    python::lockGIL();


    // run code
    PyRun_SimpleString(pycode.c_str());


    // get object from main dict
    auto mod = python::getMainModule();
    auto pipelineFunc = PyObject_GetAttrString(mod, pipelineFuncName.c_str());
    ASSERT_TRUE(PyCallable_Check(pipelineFunc));


//    // define here row object
//    Row row(10, 20);
//    // call function and fetch result
//    auto args = PyTuple_New(1);
//    PyTuple_SetItem(args, 0, python::rowToPython(row));
//    auto resObj = PyObject_Call(pipelineFunc, args, nullptr);
//
//    PyObject_Print(resObj, stdout, 0);
//    std::cout<<std::endl;
//
//#warning "no tests here in interest of time wrt paper"

    python::closeInterpreter();
}

TEST(PythonPipeline, BasicJoin) {
    using namespace tuplex;
    using namespace std;

    python::initInterpreter();
    python::unlockGIL();

    PythonPipelineBuilder ppb("pipeline");
    ppb.csvInput(1001, {"a", "b", "c", "d"});
    ppb.innerJoinDict(1002, "hashmap1", option<std::string>("a"), {"value"});
    ppb.tuplexOutput(1003, python::Type::UNKNOWN);
    auto code = ppb.getCode();

    cout<<"PIPELINE code\n"<<core::withLineNumbers(code)<<endl;

    python::lockGIL();

    auto pipFunction = compile_pipeline(code);

    // call python fallback pipeline function with a) input csv string b) demo hashmap
    auto inputStr = python::PyString_FromString("test,20,30,40");
    HashTableSink hsink;
    hsink.null_bucket = nullptr;
    hsink.hm = hashmap_new();
    auto hm_wrapped = CreatePythonHashMapWrapper(hsink, python::Type::STRING,
                                                 python::Type::I64, LookupStorageMode::LISTOFVALUES);

    // add some data to the hashmap (can use any key/val combo)
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("i64"), PyLong_FromLong(1));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("i64"), PyLong_FromLong(2));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("f64"), PyFloat_FromDouble(41.5));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("f64"), PyFloat_FromDouble(-3.141));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("str"), python::PyString_FromString("hello world!"));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("null"), python::none());

    // now the test objects (multiple data)
    // => note: same key, different bucket needs combined result!
    // Note: if column names are intended, then create Row objects!
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("test"), PyLong_FromLong(42));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("test"), python::boolean(true));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("test"), PyFloat_FromDouble(3.141));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("test"), python::PyString_FromString("hello"));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("test"), python::none());
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("test"), PyTuple_New(0)); // ()
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("test"), PyList_New(0));  // []
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), python::PyString_FromString("test"), PyDict_New());          // {}

    // test added buckets
    auto b1 = PyObject_GetItem(reinterpret_cast<PyObject *>(hm_wrapped), python::PyString_FromString("i64"));
    ASSERT_TRUE(b1);
    EXPECT_EQ(python::PyString_AsString(b1), "[(1,), (2,)]");
    auto b2 = PyObject_GetItem(reinterpret_cast<PyObject *>(hm_wrapped), python::PyString_FromString("f64"));
    ASSERT_TRUE(b2);
    EXPECT_EQ(python::PyString_AsString(b2), "[(41.5,), (-3.141,)]");
    auto b3 = PyObject_GetItem(reinterpret_cast<PyObject *>(hm_wrapped), python::PyString_FromString("str"));
    ASSERT_TRUE(b3);
    EXPECT_EQ(python::PyString_AsString(b3), "[('hello world!',)]");
    auto b4 = PyObject_GetItem(reinterpret_cast<PyObject *>(hm_wrapped), python::PyString_FromString("null"));
    ASSERT_TRUE(b4);
    EXPECT_EQ(python::PyString_AsString(b4), "[(None,)]");

    auto b5 = PyObject_GetItem(reinterpret_cast<PyObject *>(hm_wrapped), python::PyString_FromString("test"));
    ASSERT_TRUE(b5);
    EXPECT_EQ(python::PyString_AsString(b5), "[(42,), (True,), (3.141,), ('hello',), (None,), ((),), ([],), ({},)]"); // @TODO:

    auto args = PyTuple_New(2);
    PyTuple_SET_ITEM(args, 0, inputStr);
    PyTuple_SET_ITEM(args, 1, (PyObject*)hm_wrapped);
    auto resObj = PyObject_Call(pipFunction, args, nullptr);
    ASSERT_TRUE(resObj);
    PyObject_Print(resObj, stdout, 0);
    cout<<endl;

    // convert res object to rows & validate them!
    vector<Row> output_rows;
    vector<std::string> output_cols;
    auto py_rows = PyObject_GetItem(resObj, python::PyString_FromString("outputRows"));
    auto py_cols = PyObject_GetItem(resObj, python::PyString_FromString("outputColumns"));
    ASSERT_TRUE(py_rows); ASSERT_TRUE(py_cols);
    for(int i = 0; i < PyObject_Length(py_rows); ++i) {
        output_rows.push_back(python::pythonToRow(PyList_GetItem(py_rows, i)));
    }
    for(int i = 0; i < PyObject_Length(py_cols); ++i) {
        output_cols.push_back(python::PyString_AsString(PyTuple_GetItem(py_cols, i)));
    }

    ASSERT_EQ(output_cols.size(), 5);
    EXPECT_EQ(output_cols[0], "b");
    EXPECT_EQ(output_cols[1], "c");
    EXPECT_EQ(output_cols[2], "d");
    EXPECT_EQ(output_cols[3], "a");
    EXPECT_EQ(output_cols[4], "value");

    ASSERT_EQ(output_rows.size(), 8);

    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,"test",42));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,"test",true));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,"test",3.141));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,"test","hello"));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,"test",Field::null()));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,"test",Field::empty_tuple()));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,"test",Field::empty_list()));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,"test",Field::empty_dict()));

    python::closeInterpreter();
}

TEST(PythonPipeline, BasicIntJoin) {
    using namespace tuplex;
    using namespace std;

    python::initInterpreter();
    python::unlockGIL();

    PythonPipelineBuilder ppb("pipeline");
    ppb.csvInput(1001, {"a", "b", "c", "d"});
    ppb.innerJoinDict(1002, "hashmap1", option<std::string>("a"), {"value"});
    ppb.tuplexOutput(1003, python::Type::UNKNOWN);
    auto code = ppb.getCode();

    cout<<"PIPELINE code\n"<<core::withLineNumbers(code)<<endl;

    python::lockGIL();

    auto pipFunction = compile_pipeline(code);

    // call python fallback pipeline function with a) input csv string b) demo hashmap
    auto inputStr = python::PyString_FromString("10,20,30,40");
    HashTableSink hsink;
    hsink.null_bucket = nullptr;
    hsink.hm = hashmap_new();
    auto hm_wrapped = CreatePythonHashMapWrapper(hsink, python::Type::I64, python::Type::I64, LookupStorageMode::LISTOFVALUES);

    // add some data to the hashmap (can use any key/val combo)
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(1), PyLong_FromLong(1));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(1), PyLong_FromLong(2));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(2), PyFloat_FromDouble(41.5));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(2), PyFloat_FromDouble(-3.141));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(3), python::PyString_FromString("hello world!"));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(4), python::none());

    // now the test objects (key 10, multiple data)
    // => note: same key, different bucket needs combined result!
    // Note: if column names are intended, then create Row objects!
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(10), PyLong_FromLong(42));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(10), python::boolean(true));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(10), PyFloat_FromDouble(3.141));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(10), python::PyString_FromString("hello"));
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(10), python::none());
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(10), PyTuple_New(0)); // ()
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(10), PyList_New(0));  // []
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrapped), PyLong_FromLong(10), PyDict_New());          // {}

    // test added buckets
    auto b1 = PyObject_GetItem(reinterpret_cast<PyObject *>(hm_wrapped), PyLong_FromLong(1));
    ASSERT_TRUE(b1);
    EXPECT_EQ(python::PyString_AsString(b1), "[(1,), (2,)]");
    auto b2 = PyObject_GetItem(reinterpret_cast<PyObject *>(hm_wrapped), PyLong_FromLong(2));
    ASSERT_TRUE(b2);
    EXPECT_EQ(python::PyString_AsString(b2), "[(41.5,), (-3.141,)]");
    auto b3 = PyObject_GetItem(reinterpret_cast<PyObject *>(hm_wrapped), PyLong_FromLong(3));
    ASSERT_TRUE(b3);
    EXPECT_EQ(python::PyString_AsString(b3), "[('hello world!',)]");
    auto b4 = PyObject_GetItem(reinterpret_cast<PyObject *>(hm_wrapped), PyLong_FromLong(4));
    ASSERT_TRUE(b4);
    EXPECT_EQ(python::PyString_AsString(b4), "[(None,)]");

    auto b5 = PyObject_GetItem(reinterpret_cast<PyObject *>(hm_wrapped), PyLong_FromLong(10));
    ASSERT_TRUE(b5);
    EXPECT_EQ(python::PyString_AsString(b5), "[(42,), (True,), (3.141,), ('hello',), (None,), ((),), ([],), ({},)]"); // @TODO:

    auto args = PyTuple_New(2);
    PyTuple_SET_ITEM(args, 0, inputStr);
    PyTuple_SET_ITEM(args, 1, (PyObject*)hm_wrapped);
    auto resObj = PyObject_Call(pipFunction, args, nullptr);
    ASSERT_TRUE(resObj);
    PyObject_Print(resObj, stdout, 0);
    cout<<endl;

    // convert res object to rows & validate them!
    vector<Row> output_rows;
    vector<std::string> output_cols;
    auto py_rows = PyObject_GetItem(resObj, python::PyString_FromString("outputRows"));
    auto py_cols = PyObject_GetItem(resObj, python::PyString_FromString("outputColumns"));
    ASSERT_TRUE(py_rows); ASSERT_TRUE(py_cols);
    for(int i = 0; i < PyObject_Length(py_rows); ++i) {
        output_rows.push_back(python::pythonToRow(PyList_GetItem(py_rows, i)));
    }
    for(int i = 0; i < PyObject_Length(py_cols); ++i) {
        output_cols.push_back(python::PyString_AsString(PyTuple_GetItem(py_cols, i)));
    }

    ASSERT_EQ(output_cols.size(), 5);
    EXPECT_EQ(output_cols[0], "b");
    EXPECT_EQ(output_cols[1], "c");
    EXPECT_EQ(output_cols[2], "d");
    EXPECT_EQ(output_cols[3], "a");
    EXPECT_EQ(output_cols[4], "value");

    ASSERT_EQ(output_rows.size(), 8);

    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,10,42));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,10,true));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,10,3.141));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,10,"hello"));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,10,Field::null()));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,10,Field::empty_tuple()));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,10,Field::empty_list()));
    EXPECT_IN_VECTOR(output_rows, Row(20,30,40,10,Field::empty_dict()));

    python::closeInterpreter();
}

TEST(PythonPipeline, LeftJoin) {
    using namespace tuplex;
    using namespace std;

    python::initInterpreter();
    python::unlockGIL();

    PythonPipelineBuilder ppb("pipeline");
    ppb.csvInput(1001, {"a", "b", "c"});
    ppb.leftJoinDict(1002, "hashmap1", option<std::string>("a"), {"value"});
    ppb.tuplexOutput(1004, python::Type::UNKNOWN);
    auto code = ppb.getCode();

    cout << "PIPELINE code\n" << core::withLineNumbers(code) << endl;

    python::lockGIL();


    auto pipFunction = compile_pipeline(code);

    // call python fallback pipeline function with a) input csv string b) demo hashmap
    HashTableSink hsink;
    hsink.null_bucket = nullptr;
    hsink.hm = hashmap_new();

    auto hm_wrapped = CreatePythonHashMapWrapper(hsink, python::Type::STRING, python::Type::I64, LookupStorageMode::LISTOFVALUES);

    // put some conforming/non-conforming data into the buckets
    PyObject_SetItem(reinterpret_cast<PyObject *>(hm_wrapped), python::PyString_FromString("abc"),
                     PyLong_FromLong(200)); // regular data, as specified in normal case
    PyObject_SetItem(reinterpret_cast<PyObject *>(hm_wrapped), PyLong_FromLong(10),
                     PyLong_FromLong(400)); // wrong key-type, right bucket-type

    vector<string> testData = {"abc,100,1000",
                               "10,110,1100",
                               "False,120,1200"};

    // call pipeline w. test object
    vector<vector<Row>> res;
    for (const auto &line : testData) {
        res.emplace_back(call_pipeline(line, pipFunction, {reinterpret_cast<PyObject *>(hm_wrapped),}));
    }

    // print everything
    cout << "========\nResults:\n========\n";
    for (auto v : res)
        for (auto r: v)
            cout << r.toPythonString() << endl;

    ASSERT_EQ(res.size(), 3); // 3 input rows
    ASSERT_EQ(res[0].size(), 1);
    EXPECT_EQ(res[0][0], Row(100, 1000, "abc", 200));
    ASSERT_EQ(res[1].size(), 1);
    EXPECT_EQ(res[1][0], Row(110, 1100, 10, 400));
    ASSERT_EQ(res[2].size(), 1);
    EXPECT_EQ(res[2][0], Row(120, 1200, false, Field::null()));

    python::closeInterpreter();
}

TEST(PythonPipeline, LeftIntJoin) {
    using namespace tuplex;
    using namespace std;

    python::initInterpreter();
    python::unlockGIL();

    PythonPipelineBuilder ppb("pipeline");
    ppb.csvInput(1001, {"a", "b", "c"});
    ppb.leftJoinDict(1002, "hashmap1", option<std::string>("a"), {"value"});
    ppb.tuplexOutput(1004, python::Type::UNKNOWN);
    auto code = ppb.getCode();

    cout << "PIPELINE code\n" << core::withLineNumbers(code) << endl;

    python::lockGIL();


    auto pipFunction = compile_pipeline(code);

    // call python fallback pipeline function with a) input csv string b) demo hashmap
    HashTableSink hsink;
    hsink.null_bucket = nullptr;
    hsink.hm = hashmap_new();

    auto hm_wrapped = CreatePythonHashMapWrapper(hsink, python::Type::I64, python::Type::I64, LookupStorageMode::LISTOFVALUES);

    // put some conforming/non-conforming data into the buckets
    PyObject_SetItem(reinterpret_cast<PyObject *>(hm_wrapped), PyLong_FromLong(10),
                     PyLong_FromLong(400)); // regular data, as specified in normal case
    PyObject_SetItem(reinterpret_cast<PyObject *>(hm_wrapped), python::PyString_FromString("abc"),
                     PyLong_FromLong(200)); // wrong key-type, right bucket-type

    vector<string> testData = {"10,110,1100",
                               "abc,100,1000",
                               "False,120,1200"};

    // call pipeline w. test object
    vector<vector<Row>> res;
    for (const auto &line : testData) {
        res.emplace_back(call_pipeline(line, pipFunction, {reinterpret_cast<PyObject *>(hm_wrapped),}));
    }

    // print everything
    cout << "========\nResults:\n========\n";
    for (auto v : res)
        for (auto r: v)
            cout << r.toPythonString() << endl;

    ASSERT_EQ(res.size(), 3); // 3 input rows
    ASSERT_EQ(res[0].size(), 1);
    EXPECT_EQ(res[0][0], Row(110, 1100, 10, 400));
    ASSERT_EQ(res[1].size(), 1);
    EXPECT_EQ(res[1][0], Row(100, 1000, "abc", 200));
    ASSERT_EQ(res[2].size(), 1);
    EXPECT_EQ(res[2][0], Row(120, 1200, false, Field::null()));

    python::closeInterpreter();
}


/// test multiple join operators
/// I.e. essentially this tests that a long running pipeline works...
TEST(PythonPipeline, MultiJoin) {
    using namespace tuplex;
    using namespace std;

    python::initInterpreter();
    python::unlockGIL();

    PythonPipelineBuilder ppb("pipeline");
    ppb.csvInput(1001, {"a", "b", "c"});
    ppb.innerJoinDict(1002, "hashmap1", option<std::string>("a"), {"value"});
    ppb.mapColumn(1010, "c", UDF("lambda x: x + 1"));
    ppb.innerJoinDict(1003, "hashmap2", option<std::string>("a"), {"bucket"});
    ppb.tuplexOutput(1004, python::Type::UNKNOWN);
    auto code = ppb.getCode();

    cout << "PIPELINE code\n" << core::withLineNumbers(code) << endl;

    python::lockGIL();


    auto pipFunction = compile_pipeline(code);

    // call python fallback pipeline function with a) input csv string b) demo hashmap
    HashTableSink hsinkA;
    hsinkA.null_bucket = nullptr;
    hsinkA.hm = hashmap_new();
    HashTableSink hsinkB;
    hsinkB.null_bucket = nullptr;
    hsinkB.hm = hashmap_new();

    auto hm_wrappedA = CreatePythonHashMapWrapper(hsinkA, python::Type::STRING, python::Type::I64, LookupStorageMode::LISTOFVALUES);
    auto hm_wrappedB = CreatePythonHashMapWrapper(hsinkB, python::Type::STRING, python::Type::I64, LookupStorageMode::LISTOFVALUES);

    // put some conforming/non-conforming data into the buckets
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrappedA), python::PyString_FromString("abc"), PyLong_FromLong(200)); // regular data, as specified in normal case
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrappedA), PyLong_FromLong(10), PyLong_FromLong(400)); // wrong key-type, right bucket-type
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrappedA), python::PyString_FromString("xyz"), python::PyString_FromString("wrong_bucket_val"));  // right key, wrong bucket value
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrappedA), PyLong_FromLong(10), python::PyString_FromString("blub")); // wrong key-type, wrong bucket-type


    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrappedB), python::PyString_FromString("abc"), PyLong_FromLong(42)); // regular data, as specified in normal case
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrappedB), PyLong_FromLong(10), PyLong_FromLong(99)); // wrong key-type, right bucket-type
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrappedB), python::PyString_FromString("xyz"), python::PyString_FromString("bla"));  // right key, wrong bucket value
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrappedB), PyLong_FromLong(10), python::PyString_FromString("foo")); // wrong key-type, wrong bucket-type
    PyObject_SetItem(reinterpret_cast<PyObject*>(hm_wrappedB), python::PyString_FromString("xyz"), python::boolean(false));  // another combo to spice it up

    vector<string> testData = {"abc,100,1000",
                               "10,110,1100",
                               "False,120,1200",
                               "xyz,130,1300"};

    // call pipeline w. test object
    vector<vector<Row>> res;
    for(const auto& line : testData) {
        res.emplace_back(call_pipeline(line, pipFunction, {reinterpret_cast<PyObject*>(hm_wrappedA),
                                                           reinterpret_cast<PyObject*>(hm_wrappedB)}));
    }

    // print everything
    cout<<"========\nResults:\n========\n";
    for(auto v : res)
        for(auto r: v)
            cout<<r.toPythonString()<<endl;

    ASSERT_EQ(res.size(), 4);

    // abc,100,1000
    ASSERT_EQ(res[0].size(), 1);
    EXPECT_EQ(res[0][0], Row(100,1001,200,"abc",42));

    ASSERT_EQ(res[1].size(), 4); // two rows work for that key in each join...
    EXPECT_EQ(res[1][0], Row(110,1101,400,10,99 ));
    EXPECT_EQ(res[1][1], Row(110,1101,400,10,"foo" ));
    EXPECT_EQ(res[1][2], Row(110,1101,"blub",10,99));
    EXPECT_EQ(res[1][3], Row(110,1101,"blub",10,"foo"));

    ASSERT_EQ(res[2].size(), 0); // no match

    ASSERT_EQ(res[3].size(), 2);
    EXPECT_EQ(res[3][0], Row(130,1301,"wrong_bucket_val", "xyz", "bla"));
    EXPECT_EQ(res[3][1], Row(130,1301,"wrong_bucket_val", "xyz", false));

    python::closeInterpreter();
}

TEST(PythonPipeline, ResolveAndIgnore) {
    using namespace tuplex;

    auto extractBd_c = "def extractBd(x):\n"
                       "   val = x['facts and features']\n"
                       "   max_idx = val.find(' bd')\n"
                       "   if max_idx < 0:\n"
                       "       max_idx = len(val)\n"
                       "   s = val[:max_idx]\n"
                       "   # find comma before\n"
                       "   split_idx = s.rfind(',')\n"
                       "   if split_idx < 0:\n"
                       "       split_idx = 0\n"
                       "   else:\n"
                       "       split_idx += 2\n"
                       "   r = s[split_idx:]\n"
                       "   return int(r)";

    auto resolveBd_c = "def resolveBd(x):\n"
                              "    if 'Studio' in x['facts and features']:\n"
                              "        return 1\n"
                              "    raise ValueError\n";

    python::initInterpreter();

    python::unlockGIL();

    PythonPipelineBuilder ppb("pipeline");
    ppb.csvInput(100);
    ppb.withColumn(101, "bedrooms", UDF(extractBd_c, ""));
    ppb.resolve(102, ExceptionCode::VALUEERROR, UDF(resolveBd_c, ""));
    ppb.ignore(103, ExceptionCode::VALUEERROR);
    ppb.mapOperation(104, UDF("lambda x: x['bedrooms']", "bedrooms"));
    ppb.csvOutput();
    auto py_code = ppb.getCode();
    std::cout<<"\n"<<py_code<<std::endl;

    python::lockGIL();

    python::closeInterpreter();
}