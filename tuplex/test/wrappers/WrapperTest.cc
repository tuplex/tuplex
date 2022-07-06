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
#include <PythonDataSet.h>
#include <PythonContext.h>
#include <PythonHelpers.h>
#include <PythonWrappers.h>
#include <StringUtils.h>
#include <CSVUtils.h>
#include <VirtualFileSystem.h>
#include <parser/Parser.h>
#include "../core/TestUtils.h"

#include <boost/filesystem/operations.hpp>

// need for these tests a running python interpreter, so spin it up
class WrapperTest : public TuplexTest {
    void SetUp() override {
        TuplexTest::SetUp();

        python::initInterpreter();
        assert(python::holdsGIL());
    }

    void TearDown() override {
        TuplexTest::TearDown();
        python::closeInterpreter();
    }
};

#ifdef BUILD_WITH_AWS
TEST_F(WrapperTest, LambdaBackend) {
    using namespace tuplex;

    // disable for now
    GTEST_SKIP();


    // activate AWS lambda backend
    PythonContext c("python", "", "{\"backend\":\"lambda\"}");
}
#endif

// Important detail: RAII of boost python requires call to all boost::python destructors before closing the interpreter.

TEST_F(WrapperTest, BasicMergeInOrder) {
    using namespace tuplex;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    PythonContext c("c", "", opts.asJSON());

    auto listSize = 30000;
    auto listObj = PyList_New(listSize);
    auto expectedResult = PyList_New(listSize);
    for (int i = 0; i < listSize; ++i) {
        if (i % 3 == 0) {
            PyList_SetItem(listObj, i, PyLong_FromLong(0));
            PyList_SetItem(expectedResult, i, PyLong_FromLong(-1));
        } else if (i % 5 == 0) {
            PyList_SetItem(listObj, i, python::PyString_FromString(std::to_string(i).c_str()));
            PyList_SetItem(expectedResult, i, python::PyString_FromString(std::to_string(i).c_str()));
        } else {
            PyList_SetItem(listObj, i, PyLong_FromLong(i));
            PyList_SetItem(expectedResult, i, PyLong_FromLong(i));
        }
    }

    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).map("lambda x: 1 // x if x == 0 else x", "").resolve(ecToI64(ExceptionCode::ZERODIVISIONERROR), "lambda x: -1", "").collect();
        auto resObj = res.ptr();

        ASSERT_EQ(PyList_Size(resObj), PyList_Size(expectedResult));
        for (int i = 0; i < PyList_Size(expectedResult); ++i) {
            EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, i)).toPythonString(), python::pythonToRow(
                    PyList_GetItem(expectedResult, i)).toPythonString());
        }
    }
}

TEST_F(WrapperTest, StringTuple) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject *listObj = PyList_New(4);
    PyObject *tupleObj1 = PyTuple_New(2);
    PyTuple_SET_ITEM(tupleObj1, 0, python::PyString_FromString("a"));
    PyTuple_SET_ITEM(tupleObj1, 1, python::PyString_FromString("a"));

    PyObject *tupleObj2 = PyTuple_New(2);
    PyTuple_SET_ITEM(tupleObj2, 0, python::PyString_FromString("b"));
    PyTuple_SET_ITEM(tupleObj2, 1, python::PyString_FromString("b"));

    PyObject *tupleObj3 = PyTuple_New(2);
    PyTuple_SET_ITEM(tupleObj3, 0, python::PyString_FromString("c"));
    PyTuple_SET_ITEM(tupleObj3, 1, python::PyString_FromString("c"));

    PyObject *tupleObj4 = PyTuple_New(2);
    PyTuple_SET_ITEM(tupleObj4, 0, python::PyString_FromString("d"));
    PyTuple_SET_ITEM(tupleObj4, 1, python::PyString_FromString("d"));

    PyList_SetItem(listObj, 0, tupleObj1);
    PyList_SetItem(listObj, 1, tupleObj2);
    PyList_SetItem(listObj, 2, tupleObj3);
    PyList_SetItem(listObj, 3, tupleObj4);

    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).map("lambda x: x", "").collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        // Change to 4 when parallelize changes are merged
        ASSERT_EQ(PyList_GET_SIZE(resObj), 4);

        PyObject_Print(resObj, stdout, 0);
    }
}

TEST_F(WrapperTest, MixedSimpleTupleTuple) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject *listObj = PyList_New(4);
    PyObject *tupleObj1 = PyTuple_New(2);
    PyTuple_SET_ITEM(tupleObj1, 0, python::PyString_FromString("a"));
    PyTuple_SET_ITEM(tupleObj1, 1, PyLong_FromLong(1));

    PyObject *tupleObj2 = PyTuple_New(2);
    PyTuple_SET_ITEM(tupleObj2, 0, python::PyString_FromString("b"));
    PyTuple_SET_ITEM(tupleObj2, 1, PyLong_FromLong(2));

    PyObject *tupleObj3 = PyTuple_New(2);
    PyTuple_SET_ITEM(tupleObj3, 0, python::PyString_FromString("c"));
    PyTuple_SET_ITEM(tupleObj3, 1, PyLong_FromLong(3));

    PyObject *tupleObj4 = PyTuple_New(2);
    PyTuple_SET_ITEM(tupleObj4, 0, Py_None);
    PyTuple_SET_ITEM(tupleObj4, 1, PyLong_FromLong(4));

    PyList_SetItem(listObj, 0, tupleObj1);
    PyList_SetItem(listObj, 1, tupleObj2);
    PyList_SetItem(listObj, 2, tupleObj3);
    PyList_SetItem(listObj, 3, tupleObj4);

    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 4);

        PyObject_Print(resObj, stdout, 0);
    }
}

TEST_F(WrapperTest, StringParallelize) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = PyList_New(3);
    PyList_SET_ITEM(listObj, 0, python::PyString_FromString("Hello"));
    PyList_SET_ITEM(listObj, 1, python::PyString_FromString("world"));
    PyList_SET_ITEM(listObj, 2, python::PyString_FromString("!"));

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 3);

        // check contents

    }
}

TEST_F(WrapperTest, DictionaryParallelize) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * dictObj1 = PyDict_New();
    PyDict_SetItem(dictObj1, python::PyString_FromString("a"), PyFloat_FromDouble(0.0));
    PyDict_SetItem(dictObj1, python::PyString_FromString("b"), PyFloat_FromDouble(1.345));
    PyDict_SetItem(dictObj1, python::PyString_FromString("c"), PyFloat_FromDouble(-2.234));

    PyObject * dictObj2 = PyDict_New();
    PyDict_SetItem(dictObj2, python::PyString_FromString("d"), PyFloat_FromDouble(1.23));
    PyDict_SetItem(dictObj2, python::PyString_FromString("e"), PyFloat_FromDouble(2.34));
    PyDict_SetItem(dictObj2, python::PyString_FromString("f"), PyFloat_FromDouble(-3.45));

    PyObject * listObj = PyList_New(2);
    PyList_SET_ITEM(listObj, 0, dictObj1);
    PyList_SET_ITEM(listObj, 1, dictObj2);

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_Size(resObj), 2);

        // check contents
        // ==> there will be only one result though, because of the type inference. Basically the first row will be taken :)
        // --> the other row will be stored as bad input row.
        auto tuple1 = PyList_GetItem(resObj, 0);
        ASSERT_TRUE(PyTuple_Check(tuple1));
        ASSERT_EQ(PyTuple_Size(tuple1), 6);
    }
}

TEST_F(WrapperTest, SimpleCSVParse) {
    using namespace tuplex;

    // write sample file
    auto fName = testName + ".csv";
    FILE *f = fopen(fName.c_str(), "w");
    fprintf(f, "1,2,3,FAST ETL!\n");
    fprintf(f, "4,5,6,FAST ETL!\n");
    fprintf(f, "7,8,9,\"FAST ETL!\"");
    fclose(f);

    PyObject * pyopt = PyDict_New();
    PyDict_SetItemString(pyopt, "tuplex.webui.enable", Py_False);

    // RAII, destruct python context!
    PythonContext c("c", "", microTestOptions().asJSON());

    // weird block syntax due to RAII problems.
    {
        // below is essentially the following python code.
        // res = dataset.map(lambda a, b, c, d: d).collect()
        // assert res == ["FAST ETL!", "FAST ETL!", "FAST ETL!"]
        auto res = c.csv(testName + ".csv").map("lambda a, b, c, d: d", "").collect();

        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 3);

        // check contents
        for (int i = 0; i < 3; ++i) {
            PyObject * obj = PyList_GET_ITEM(resObj, i);
            ASSERT_TRUE(PyUnicode_Check(obj));
            auto content = std::string(PyUnicode_AsUTF8(obj));
            std::cout << "row " << i + 1 << ": " << content << std::endl;
            EXPECT_EQ(content, "FAST ETL!");
        }
    }

    // remove file
    remove(fName.c_str());
}

TEST_F(WrapperTest, GetOptions) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    // weird RAII problems of boost python
    {
        auto d = c.options();

        ContextOptions co = ContextOptions::defaults();
        auto length = py::len(c.options());
        EXPECT_GE(co.store().size(), length);
    }

}

TEST_F(WrapperTest, TwoContexts) {
    using namespace tuplex;

    PythonContext c("", "", microTestOptions().asJSON());
    PythonContext c2("", "", microTestOptions().asJSON());

    {
        auto opt1 = c.options();
        auto op2 = c2.options();
    }
}

TEST_F(WrapperTest, Show) {
    using namespace tuplex;

    // write sample file
    auto fName = testName + ".csv";
    FILE *f = fopen(fName.c_str(), "w");
    fprintf(f, "a,b,c,s\n");
    fprintf(f, "1,2,3,FAST ETL!\n");
    fprintf(f, "4,5,6,FAST ETL!\n");
    fprintf(f, "7,8,9,\"FAST ETL!\"");
    fclose(f);

    PyObject * pyopt = PyDict_New();
    PyDict_SetItemString(pyopt, "tuplex.webui.enable", Py_False);

    // RAII, destruct python context!
    PythonContext c("python", "", microTestOptions().asJSON());

    // weird block syntax due to RAII problems.
    {
        // below is essentially the following python code.
        // res = dataset.map(lambda a, b, c, d: d).collect()
        // assert res == ["FAST ETL!", "FAST ETL!", "FAST ETL!"]
        c.csv(testName + ".csv").show();
    }

    // remove file
    remove(fName.c_str());

}

TEST_F(WrapperTest, GoogleTrace) {
    using namespace tuplex;
    using namespace std;

    string sampleTraceFile = "../resources/gtrace-jobevents-sample.csv";

    PyObject * pyopt = PyDict_New();
    PyDict_SetItemString(pyopt, "tuplex.webui.enable", Py_False);

    // RAII, destruct python context!
    PythonContext c("python", "", testOptions().asJSON());
    /// Based on Google trace data, this mini pipeline serves as CSV parsing test ground.
    ///  c.csv(file_path) \
    ///   .filter(lambda x: x[3] == 0) \
    ///   .selectColumns([2, 0]).collect()

    // weird block syntax due to RAII problems.
    {
        PyObject * listObj = PyList_New(2);
        PyList_SET_ITEM(listObj, 0, PyLong_FromLong(2));
        PyList_SET_ITEM(listObj, 1, PyLong_FromLong(0));

        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.csv(sampleTraceFile) \
                    .filter("lambda x: x[3] == 0", "") \
                    .selectColumns(list).collect();

        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 10);

        // check contents
        // should be
        //3418309,0
        //3418314,0
        //3418319,0
        //3418324,0
        //3418329,0
        //3418334,0
        //3418339,0
        //3418356,0
        //3418363,0
        //3418368,0

        std::vector<int> ref{3418309, 3418314, 3418319, 3418324, 3418329, 3418334, 3418339, 3418356, 3418363, 3418368};
        for (int i = 0; i < 10; ++i) {
            PyObject * obj = PyList_GET_ITEM(resObj, i);
            ASSERT_TRUE(PyTuple_Check(obj));
            ASSERT_EQ(PyTuple_Size(obj), 2);
            auto c1 = PyTuple_GetItem(obj, 0);
            auto c2 = PyTuple_GetItem(obj, 1);

            ASSERT_TRUE(c1 && c2);
            auto i1 = PyLong_AsLong(c1);
            auto i2 = PyLong_AsLong(c2);
            EXPECT_EQ(i1, ref[i]);
            EXPECT_EQ(i2, 0);
        }
    }
}


TEST_F(WrapperTest, extractPriceExample) {
    using namespace tuplex;
    using namespace std;

    std::string udfCode = "def extractPrice(x):\n"
                          "    price = x['price']\n"
                          "\n"
                          "    if x['offer'] == 'sold':\n"
                          "        # price is to be calculated using price/sqft * sqft\n"
                          "        val = x['facts and features']\n"
                          "        s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]\n"
                          "        r = s[s.find('$')+1:s.find(', ') - 1]\n"
                          "        price_per_sqft = int(r)\n"
                          "        price = price_per_sqft * x['sqft']\n"
                          "    elif x['offer'] == 'rent':\n"
                          "        max_idx = price.rfind('/')\n"
                          "        price = int(price[1:max_idx].replace(',', ''))\n"
                          "    else:\n"
                          "        # take price from price column\n"
                          "        price = int(price[1:].replace(',', ''))\n"
                          "\n"
                          "    return price";


    // read all CSV lines in from test_data file
    auto file = VirtualFileSystem::open_file(URI("../resources/extract_price_data.csv"), VirtualFileMode::VFS_READ);
    ASSERT_TRUE(file);

    // super slow CSV to vector loading here...
    auto fileSize = file->size();
    char *buffer = new char[fileSize];
    file->read(buffer, fileSize);
    file->close();
    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> row;
    size_t numParsedBytes = 0;
    const char *ptr = buffer;
    while(ExceptionCode::SUCCESS == parseRow(ptr, buffer + fileSize, row, numParsedBytes)) {
        if(0 == numParsedBytes)
            break;
        rows.push_back(row);
        row.clear();
        ptr += numParsedBytes;
        numParsedBytes = 0;
    }
    delete [] buffer;

    // remove first row
    rows = std::vector<std::vector<std::string>>(rows.begin() + 1, rows.end());

    // convert to python object
    auto scaleFactor = 1000;
    PyObject* listObj = PyList_New(scaleFactor * rows.size());
    int pos = 0;
    for(int j = 0; j < scaleFactor; ++j) {
        for(auto r : rows) {

            PyObject* tupleObj = PyTuple_New(4);
            PyTuple_SET_ITEM(tupleObj, 0, python::PyString_FromString(r[0].c_str()));
            PyTuple_SET_ITEM(tupleObj, 1, python::PyString_FromString(r[1].c_str()));
            PyTuple_SET_ITEM(tupleObj, 2, python::PyString_FromString(r[2].c_str()));
            PyTuple_SET_ITEM(tupleObj, 3, PyLong_FromLong(std::stoi(r[3])));
            PyList_SET_ITEM(listObj, pos++, tupleObj);
        }
    }
    auto list = py::reinterpret_borrow<py::list>(listObj);

    // parallelize extract Price function & test using both dictionary AND tuple syntax.
    // input data here should be
    // price,offer,facts and features,sqft
    // "$489,000",sale,"3 bds , 1 ba , 1,560 sqft",1560
//    PyObject* listObj = PyList_New(2);
//    PyObject* tupleObj = PyTuple_New(4);
//    PyTuple_SET_ITEM(tupleObj, 0, python::PyString_FromString("$489,000"));
//    PyTuple_SET_ITEM(tupleObj, 1, python::PyString_FromString("sale"));
//    PyTuple_SET_ITEM(tupleObj, 2, python::PyString_FromString("3 bds , 1 ba , 1,560 sqft"));
//    PyTuple_SET_ITEM(tupleObj, 3, PyLong_FromLong(1560));
//    PyList_SET_ITEM(listObj, 0, tupleObj);
//    tupleObj = PyTuple_New(4);
//    PyTuple_SET_ITEM(tupleObj, 0, python::PyString_FromString("$489,000"));
//    PyTuple_SET_ITEM(tupleObj, 1, python::PyString_FromString("sale"));
//    PyTuple_SET_ITEM(tupleObj, 2, python::PyString_FromString("3 bds , 1 ba , 1,560 sqft"));
//    PyTuple_SET_ITEM(tupleObj, 3, PyLong_FromLong(1560));
//    PyList_SET_ITEM(listObj, 1, tupleObj);
//    auto list = py::list(py::handle<>(listObj));

    PyObject* colObj = PyList_New(4);
    PyList_SET_ITEM(colObj, 0, python::PyString_FromString("price"));
    PyList_SET_ITEM(colObj, 1, python::PyString_FromString("offer"));
    PyList_SET_ITEM(colObj, 2, python::PyString_FromString("facts and features"));
    PyList_SET_ITEM(colObj, 3, python::PyString_FromString("sqft"));
    auto cols = py::reinterpret_borrow<py::list>(colObj);

    // RAII, destruct python context!
    PythonContext c("python", "", testOptions().asJSON());

    {
        // all calls go here...
        auto res = c.parallelize(list, cols).withColumn("price", udfCode.c_str(), "").collect();
        auto resObj = res.ptr();

        //PyObject_Print(resObj, stdout, 0); std::cout<<std::endl;
        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), rows.size() * scaleFactor);
    }
}

TEST(Python, FastTupleConstruction) {
    using namespace tuplex;

    python::initInterpreter();
    std::cout<<"\n-------------"<<std::endl;

    // Note: Eliminating these additiona if stmts & Co, gives maybe a tiny speedup of
    // 5-10%. ==> worth it?? prob not really...

    int N = 1001000;

    Timer timer;
    auto listObj = PyList_New(N);
    for(int i = 0; i < N; ++i) {
        auto tupleObj = PyTuple_New(4);
        PyTuple_SET_ITEM(tupleObj, 0, python::PyString_FromString("hello world"));
        PyTuple_SET_ITEM(tupleObj, 1, python::PyString_FromString("hello whkljdkhjorld"));
        PyTuple_SET_ITEM(tupleObj, 2, python::PyString_FromString("dkfjopdjfophjhello world"));
        PyTuple_SET_ITEM(tupleObj, 3, PyLong_FromLongLong(12345));
        PyList_SET_ITEM(listObj, i, tupleObj);
    }
    std::cout<<"w/o opt took: "<<timer.time()<<"s "<<std::endl;
    timer.reset();
    listObj = PyList_New(N);
    unsigned tupleSize = 4;
    for(int i = 0; i < N; ++i) {

        // directly call python functions without all the crap
        auto tp = (PyTupleObject*)PyObject_GC_NewVar(PyTupleObject, &PyTuple_Type, tupleSize);
        PyObject_GC_Track(tp);
        if(!tp) // out of mem..
            break;

        tp->ob_item[0] = python::PyString_FromString("hello world");
        tp->ob_item[1] = python::PyString_FromString("hello whkljdkhjorld");
        tp->ob_item[2] = python::PyString_FromString("dkfjopdjfophjhello world");
        tp->ob_item[3] = PyLong_FromLongLong(12345);

        PyList_SET_ITEM(listObj, i, (PyObject*)tp);
    }
    std::cout<<"tuple construction optimized, took: "<<timer.time()<<"s "<<std::endl;

    timer.reset();
    PyListObject* lo = PyObject_GC_New(PyListObject, &PyList_Type);
    lo->ob_item = (PyObject**)PyMem_Calloc(N, sizeof(PyObject*));
    // TODO: mem check...
    Py_SIZE(lo) = N;
    lo->allocated = N;
    PyObject_GC_Track(lo);
    for(int i = 0; i < N; ++i) {

        // directly call python functions without all the crap
        auto tp = (PyTupleObject*)PyObject_GC_NewVar(PyTupleObject, &PyTuple_Type, tupleSize);
        PyObject_GC_Track(tp);
        if(!tp) // out of mem..
            break;

        tp->ob_item[0] = python::PyString_FromString("hello world");
        tp->ob_item[1] = python::PyString_FromString("hello whkljdkhjorld");
        tp->ob_item[2] = python::PyString_FromString("dkfjopdjfophjhello world");
        tp->ob_item[3] = PyLong_FromLongLong(12345);

        lo->ob_item[i] = (PyObject*)tp;
    }
    std::cout<<"tuple + list construction optimized, took: "<<timer.time()<<"s "<<std::endl;

    std::cout<<"-------------"<<std::endl;
    python::closeInterpreter();
}


// @TODO: add test for all the helpers with wrongly formatted input rows...
// i.e. check transfer to string etc.
// ==> important!!!

TEST_F(WrapperTest, ExplicitSchemaForParallelize) {
    using namespace std;

    // TODO: write a test for this, left out to make more progress towards a paper...

}

TEST_F(WrapperTest, DictListParallelize) {
    using namespace std;
    using namespace tuplex;

    // RAII, destruct python context!
    PythonContext c("python", "", microTestOptions().asJSON());

    // weird block syntax due to RAII problems.
    {
        PyObject * listObj = PyList_New(2);

        PyObject * rowObj1 = PyDict_New();
        PyDict_SetItemString(rowObj1, "a", PyLong_FromLong(2));
        PyDict_SetItemString(rowObj1, "b", PyLong_FromLong(5));

        PyObject * rowObj2 = PyDict_New();
        PyDict_SetItemString(rowObj2, "a", PyLong_FromLong(4));
        PyDict_SetItemString(rowObj2, "b", PyLong_FromLong(-10));

        PyList_SET_ITEM(listObj, 0, rowObj1);
        PyList_SET_ITEM(listObj, 1, rowObj2);

        PyObject_Print(listObj, stdout, 0);
        std::cout << std::endl;

        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).map("lambda x: x['a'] + x['b']", "").collect();

        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_Size(resObj), 2);
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 0)), Row(7));
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 1)), Row(4 - 10));
    }
}

TEST_F(WrapperTest, UpcastParallelizeI) {
    using namespace std;
    using namespace tuplex;

    // RAII, destruct python context!
    auto opts = microTestOptions();
    opts.set("tuplex.autoUpcast", "true");
    PythonContext c("python", "", opts.asJSON());

    // weird block syntax due to RAII problems.
    {
        PyObject * listObj = PyList_New(4);

        PyList_SET_ITEM(listObj, 0, PyFloat_FromDouble(2.0));
        PyList_SET_ITEM(listObj, 1, PyFloat_FromDouble(3.0));
        PyList_SET_ITEM(listObj, 2, Py_True); // auto upcast bool --> float
        PyList_SET_ITEM(listObj, 3, PyLong_FromLong(10)); // auto upcast int --> float

        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).map("lambda x: x * x + 1", "").collect();

        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_Size(resObj), 4);
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 0)), Row(5.0));
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 1)), Row(10.0));
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 2)), Row(2.0));
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 3)), Row(101.0));
    }
}

TEST_F(WrapperTest, UpcastParallelizeII) {
    using namespace std;
    using namespace tuplex;

    // RAII, destruct python context!
    auto opts = microTestOptions();
    opts.set("tuplex.autoUpcast", "true");
    PythonContext c("python", "", opts.asJSON());

    // weird block syntax due to RAII problems.
    {
        PyObject * listObj = PyList_New(4);

        PyList_SET_ITEM(listObj, 0, PyLong_FromLong(3));
        PyList_SET_ITEM(listObj, 1, Py_False); // auto upcast bool --> int
        PyList_SET_ITEM(listObj, 2, Py_True); // auto upcast bool --> int
        PyList_SET_ITEM(listObj, 3, PyLong_FromLong(2));

        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).map("lambda x: x * x - 1", "").collect();

        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_Size(resObj), 4);
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 0)), Row(8));
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 1)), Row(-1));
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 2)), Row(0));
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 3)), Row(3));
    }
}

TEST_F(WrapperTest, OptionListTest) {
    using namespace std;
    using namespace tuplex;

    // RAII, destruct python context!
    auto opts = testOptions();
    opts.set("tuplex.autoUpcast", "true");
    PythonContext c("python", "", opts.asJSON());

    // weird block syntax due to RAII problems.
    {
        PyObject * listObj1 = PyList_New(2);
        PyObject * listObj2 = PyList_New(2);
        PyObject * listObj3 = PyList_New(2);
        PyObject * listObj4 = PyList_New(2);
        PyObject * listObj5 = PyList_New(2);
        PyObject * tupleObj1 = PyTuple_New(1);
        PyObject * tupleObj2 = PyTuple_New(1);
        PyObject * listObj6 = PyList_New(2);

        PyList_SET_ITEM(listObj1, 0, PyLong_FromLong(1));
        PyList_SET_ITEM(listObj1, 1, PyLong_FromLong(2));
        PyList_SET_ITEM(listObj2, 0, PyLong_FromLong(3));
        PyList_SET_ITEM(listObj2, 1, PyLong_FromLong(4));
        PyList_SET_ITEM(listObj3, 0, PyLong_FromLong(5));
        PyList_SET_ITEM(listObj3, 1, PyLong_FromLong(6));
        PyList_SET_ITEM(listObj4, 0, listObj1);
        PyList_SET_ITEM(listObj4, 1, listObj2);
        PyList_SET_ITEM(listObj5, 0, listObj3);
        PyList_SET_ITEM(listObj5, 1, Py_None);
        Py_XINCREF(Py_None);
        PyTuple_SET_ITEM(tupleObj1, 0, listObj4);
        PyTuple_SET_ITEM(tupleObj2, 0, listObj5);
        PyList_SET_ITEM(listObj6, 0, tupleObj1);
        PyList_SET_ITEM(listObj6, 1, tupleObj2);

        auto list = py::reinterpret_borrow<py::list>(listObj6);
        auto res = c.parallelize(list).collect();

        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_Size(resObj), 2);
        auto rowType = python::Type::makeListType(python::Type::makeOptionType(python::Type::makeListType(python::Type::I64)));
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 0), rowType, true).toPythonString(), "([[1,2],[3,4]],)");
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 1), rowType, true).toPythonString(), "([[5,6],None],)");
    }
}

TEST_F(WrapperTest, FilterAll) {
    // c = Context()
    // ds = c.parallelize([1, 2, 3, 4, 5])
    // res = ds.filter(lambda x: x > 10).collect()
    // assert res == []
    using namespace std;
    using namespace tuplex;

    // RAII, destruct python context!
    auto opts = microTestOptions();
    opts.set("tuplex.autoUpcast", "true");
    PythonContext c("python", "", opts.asJSON());

    // weird block syntax due to RAII problems.
    {
        PyObject * listObj = PyList_New(4);

        PyList_SET_ITEM(listObj, 0, PyLong_FromLong(3));
        PyList_SET_ITEM(listObj, 1, PyLong_FromLong(3));
        PyList_SET_ITEM(listObj, 2, PyLong_FromLong(3));
        PyList_SET_ITEM(listObj, 3, PyLong_FromLong(2));

        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).filter("lambda x: x > 10", "").collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_Size(resObj), 0);
    }
}

TEST_F(WrapperTest, ColumnNames) {
    using namespace std;
    using namespace tuplex;

    // RAII, destruct python context!
    PythonContext c("python", "", microTestOptions().asJSON());

    // weird block syntax due to RAII problems.
    {
        PyObject * listObj = PyList_New(2);

        PyObject * rowObj1 = PyDict_New();
        PyDict_SetItemString(rowObj1, "a", PyLong_FromLong(2));
        PyDict_SetItemString(rowObj1, "b", PyLong_FromLong(5));

        PyObject * rowObj2 = PyDict_New();
        PyDict_SetItemString(rowObj2, "a", PyLong_FromLong(4));
        PyDict_SetItemString(rowObj2, "b", PyLong_FromLong(-10));

        PyList_SET_ITEM(listObj, 0, rowObj1);
        PyList_SET_ITEM(listObj, 1, rowObj2);

        PyObject_Print(listObj, stdout, 0);
        std::cout << std::endl;

        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res1 = c.parallelize(list).columns();

        ASSERT_EQ(py::len(res1), 2);
        std::vector<std::string> ref1{"a", "b"};
        for (int i = 0; i < py::len(res1); ++i) {
            std::string col = py::cast<std::string>(res1[i]);
            EXPECT_EQ(col, ref1[i]);
        }

        // write sample file
        auto fName = testName + ".csv";
        FILE *f = fopen(fName.c_str(), "w");
        fprintf(f, "a,b,c,d\n");
        fprintf(f, "4,5,6,FAST ETL!\n");
        fprintf(f, "7,8,9,\"FAST ETL!\"");
        fclose(f);

        auto res2 = c.csv(testName + ".csv").columns();
        ASSERT_EQ(py::len(res2), 4);
        std::vector<std::string> ref2{"a", "b", "c", "d"};
        for (int i = 0; i < py::len(res1); ++i) {
            std::string col = py::cast<std::string>(res2[i]);
            EXPECT_EQ(col, ref2[i]);
        }
    }
}



TEST_F(WrapperTest, IntegerTuple) {
    // self.c.parallelize([(1, 2), (3, 2)]) \
    //                  .withColumn('newcol', lambda a, b: (a + b)/10) \
    //                  .collect()
    using namespace std;
    using namespace tuplex;

    PyObject* pyopt = PyDict_New();
    PyDict_SetItemString(pyopt, "tuplex.webui.enable", Py_False);
    PyDict_SetItemString(pyopt, "tuplex.autoUpcast", Py_True);

    // RAII, destruct python context!
    auto opts = microTestOptions();
    opts.set("tuplex.autoUpcast", "true");
    PythonContext c("python", "", opts.asJSON());

    // weird block syntax due to RAII problems.
    {
        PyObject * listObj = PyList_New(2);

        PyList_SET_ITEM(listObj, 0, python::rowToPython(Row(1, 2)));
        PyList_SET_ITEM(listObj, 1, python::rowToPython(Row(3, 2)));

        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).withColumn("newcol", "lambda a, b: (a + b)/10", "").collect();

        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_Size(resObj), 2);

        Row r1 = python::pythonToRow(PyList_GET_ITEM(resObj, 0));
        Row r2 = python::pythonToRow(PyList_GET_ITEM(resObj, 1));

        EXPECT_EQ(r1, Row(1,2, (1 + 2) / 10.0));
        EXPECT_EQ(r2, Row(3, 2, (2  + 3 ) / 10.0 ));

        cout<<r1.toPythonString()<<endl;
        cout<<r2.toPythonString()<<endl;
    }
}

// Todo: for dict parallelize test edge case of typing.Dict[str, Any] given
// + some other when columns are explicitly given
// use simpler python logic...

std::string normalizeCol(const std::string &col) {
    std::string s;

    if(col.empty())
        return col;

    tuplex::splitString(col, '_', [&](const std::string &p) {
        s += tuplex::char2str(toupper(p[0]));
        for (int j = 1; j < p.length(); ++j)
            s += tuplex::char2str(tolower(p[j]));
    });

    return s;
}

TEST_F(WrapperTest, IfWithNull) {
    using namespace tuplex;
    using namespace std;

    string sampleFile = "../resources/flight_ifwithnulls.sample.csv"; // also fails, yeah!

    // RAII, destruct python context!
    auto opts = testOptions();
    opts.set("tuplex.useLLVMOptimizer", "false");
    opts.set("tuplex.executorCount", "0");
    PythonContext c("python", "", opts.asJSON());
    // execute mini part of pipeline and output csv to file
    // pipeline is
    // df = ctx.csv(perf_path)
    //
    //# rename weird column names from original file
    //renamed_cols = list(map(lambda c: ''.join(map(lambda w: w.capitalize(), c.split('_'))), df.columns))
    //
    //for i, c in enumerate(df.columns):
    //    df = df.renameColumn(c, renamed_cols[i])
    //
    // weird block syntax due to RAII problems.
    {
        auto pds = c.csv(sampleFile);

        auto cols = extractFromListOfStrings(pds.columns().ptr());

        vector<string> renamed_cols;
        for (auto & col : cols) {
            renamed_cols.emplace_back(normalizeCol(col));
        }

        // rename columns
        for (int i = 0; i < cols.size(); ++i)
            pds = pds.renameColumn(cols[i], renamed_cols[i]);

        auto cleanCode = "def cleanCode(t):\n"
                         "    if t[\"CancellationCode\"] == 'A':\n"
                         "        return 'carrier'\n"
                         "    elif t[\"CancellationCode\"] == 'B':\n"
                         "        return 'weather'\n"
                         "    elif t[\"CancellationCode\"] == 'C':\n"
                         "        return 'national air system'\n"
                         "    elif t[\"CancellationCode\"] == 'D':\n"
                         "        return 'security'\n"
                         "    else:\n"
                         "        return None";

        auto divertedCode = "def divertedUDF(row):\n"
                            "    diverted = row['Diverted']\n"
                            "    ccode = row['CancellationCode']\n"
                            "    if diverted:\n"
                            "        return 'diverted'\n"
                            "    else:\n"
                            "        if ccode:\n"
                            "            return ccode\n"
                            "        else:\n"
                            "            return 'None'";

        // use two test funcs.
        pds = pds.withColumn("CancellationCode", cleanCode, "");
        pds = pds.withColumn("CancellationReason", divertedCode, "");

        // this here works. it doesn't...???
        pds.tocsv(testName + ".csv");
    }

    // load file and compare
    auto contents = fileToString(testName + ".part0.csv");

    EXPECT_EQ(contents, "FlDate,OpUniqueCarrier,CancellationCode,Diverted,CancellationReason\n"
                        "2019-01-06,9E,,0.00000000,None\n"
                        "2019-01-07,9E,,1.00000000,diverted\n");
    std::cout<<contents<<std::endl;
}

TEST_F(WrapperTest, FlightData) {
    using namespace tuplex;
    using namespace std;

    string sampleFile = "../resources/flights_on_time_performance_2019_01.sample.tworows.csv"; // does not work

    // RAII, destruct python context!
    auto opts = testOptions();
    opts.set("tuplex.useLLVMOptimizer", "false");
    opts.set("tuplex.executorCount", "0");
    PythonContext c("python", "", opts.asJSON());
    // execute mini part of pipeline and output csv to file
    // pipeline is
    // df = ctx.csv(perf_path)
    //
    //# rename weird column names from original file
    //renamed_cols = list(map(lambda c: ''.join(map(lambda w: w.capitalize(), c.split('_'))), df.columns))
    //
    //for i, c in enumerate(df.columns):
    //    df = df.renameColumn(c, renamed_cols[i])
    //
    //# split into city and state!
    //df = df.withColumn('OriginCity', lambda x: x['OriginCityName'][:x['OriginCityName'].rfind(',')].strip())
    //df = df.withColumn('OriginState', lambda x: x['OriginCityName'][x['OriginCityName'].rfind(',')+1:].strip())
    //
    //df = df.withColumn('DestCity', lambda x: x['DestCityName'][:x['DestCityName'].rfind(',')].strip())
    //df = df.withColumn('DestState', lambda x: x['DestCityName'][x['DestCityName'].rfind(',')+1:].strip())
    //
    //df.tocsv("tuplex_output")
    // weird block syntax due to RAII problems.
    {

        auto pds = c.csv(sampleFile);

        auto cols = extractFromListOfStrings(pds.columns().ptr());

        // rename ok ??
        // functions: 2
        // external_functions: 5
        // instructions: 5367
        // blocks: 99

        vector<string> renamed_cols;
        for (int i = 0; i < cols.size(); ++i) {
            renamed_cols.emplace_back(normalizeCol(cols[i]));
        }
//
//        EXPECT_TRUE(std::find(renamed_cols.begin(), renamed_cols.end(), "OriginCityName") != cols.end());

        // rename columns
        for (int i = 0; i < cols.size(); ++i)
            pds = pds.renameColumn(cols[i], renamed_cols[i]);


        // withColumn things
        //pds = pds.withColumn("OriginCity", "lambda x: x['OriginCityName'][:x['OriginCityName'].rfind(',')].strip()",
//                             "");

        //pds = pds.mapColumn("CrsArrTime", "lambda x: '{:02}:{:02}'.format(int(x / 100), x % 100)", "");

        auto cleanCode = "def cleanCode(t):\n"
                         "    if t[\"CancellationCode\"] == 'A':\n"
                         "        return 'carrier'\n"
                         "    elif t[\"CancellationCode\"] == 'B':\n"
                         "        return 'weather'\n"
                         "    elif t[\"CancellationCode\"] == 'C':\n"
                         "        return 'national air system'\n"
                         "    elif t[\"CancellationCode\"] == 'D':\n"
                         "        return 'security'\n"
                         "    else:\n"
                         "        return None";

        auto divertedCode = "def divertedUDF(row):\n"
                            "    diverted = row['Diverted']\n"
                            "    ccode = row['CancellationCode']\n"
                            "    if diverted:\n"
                            "        return 'diverted'\n"
                            "    else:\n"
                            "        if ccode:\n"
                            "            return ccode\n"
                            "        else:\n"
                            "            return 'None'";

        // segfault when using these two UDFs in non-optimizer mode together...
        // df = df.withColumn('CancellationCode', cleanCode)
//         df = df.withColumn('CancellationReason', divertedUDF)

        // here is a bug b.c. no valid NSuite node was produced!!!
        pds = pds.withColumn("CancellationCode", cleanCode, "");

        // LOL with this it seems to work
        //pds = pds.selectColumns(STL_to_Python(vector<string>{"CancellationCode"}));

//        pds = pds.mapColumn("Diverted", "lambda x: True if x > 0 else False", "");
//        pds = pds.mapColumn("Cancelled", "lambda x: True if x > 0 else False", "");
        pds = pds.withColumn("CancellationReason", divertedCode, "").ignore(ecToI64(ExceptionCode::TYPEERROR));

        // this here works. it doesn't...???
        pds.show();


        // @TODO: fix here path issue
        // @TODO: fix here count of rows written!!!
        // bug in here??? i.e. output folder???
        //pds.tocsv("tuplex_output");



//
//                    .filter("lambda x: x[3] == 0", "") \
//                    .selectColumns(list).collect();


    }
}

TEST_F(WrapperTest, FlightSimulateSpark) {
    using namespace tuplex;
    using namespace std;

    // RAII, destruct python context!
    // {"webui.enable": false, "executorMemory": "6G",
    // "executorCount": 15, "driverMemory": "15G",
    // "partitionSize": "32MB", "runTimeMemory": "64MB",
    // "inputSplitSize": "64MB", "useLLVMOptimizer": true,
    // "optimizer.nullValueOptimization": true,
    // "csv.selectionPushdown": true,
    // "optimizer.generateParser": false,
    // "optimizer.mergeExceptionsInOrder": false,
    // "optimizer.filterPushdown": true}
    PythonContext ctx("python", "",
                    "{\"tuplex.webui.enable\":\"False\", \"tuplex.useLLVMOptimizer\" : \"True\","
                    " \"tuplex.optimizer.nullValueOptimization\" : \"True\","
                    " \"tuplex.optimizer.csv.selectionPushdown\" : \"True\","
                    " \"tuplex.resolveWithInterpreterOnly\":\"False\","
                    "\"tuplex.executorCount\":0,"
                    "\"tuplex.driverMemory\":\"6G\","
                    "\"tuplex.scratchDir\": \"file://" + scratchDir + "\"}");


    string bts_path = "../resources/flights_on_time_performance_2019_01.sample.csv";
    string airport_path = "../resources/pipelines/flights/GlobalAirportDatabase.txt";
    string carrier_path = "../resources/pipelines/flights/L_CARRIER_HISTORY.csv";
    {
        auto closureObject = PyDict_New();
        auto string_mod = PyImport_ImportModule("string");
        ASSERT_TRUE(string_mod);
        PyDict_SetItemString(closureObject, "string", string_mod);

        // required columns
        // time_req_cols = ['ActualElapsedTime', 'Distance', 'CancellationCode', 'DivActualElapsedTime', 'OpUniqueCarrier',
        //         'LateAircraftDelay', 'NasDelay', 'ArrDelay', 'SecurityDelay', 'CarrierDelay',
        //         'CrsArrTime', 'TaxiOut', 'CrsElapsedTime', 'WeatherDelay', 'DayOfWeek', 'DayOfMonth', 'Month', 'Year',
        //         'CrsDepTime', 'Cancelled',
        //         'Diverted', 'OriginCityName', 'AirTime', 'Origin', 'Dest', 'DestCityName',
        //         'DivReachedDest', 'TaxiIn', 'DepDelay', 'OpCarrierFlNum']
        auto time_req_cols_obj = python::runAndGet("time_req_cols = ['ActualElapsedTime', 'Distance', 'CancellationCode', 'DivActualElapsedTime', 'OpUniqueCarrier',\n"
                          "            'LateAircraftDelay', 'NasDelay', 'ArrDelay', 'SecurityDelay', 'CarrierDelay',\n"
                          "            'CrsArrTime', 'TaxiOut', 'CrsElapsedTime', 'WeatherDelay', 'DayOfWeek', 'DayOfMonth', 'Month', 'Year',\n"
                          "            'CrsDepTime', 'Cancelled',\n"
                          "            'Diverted', 'OriginCityName', 'AirTime', 'Origin', 'Dest', 'DestCityName',\n"
                          "            'DivReachedDest', 'TaxiIn', 'DepDelay', 'OpCarrierFlNum']", "time_req_cols");
        auto time_req_cols = py::reinterpret_borrow<py::list>(time_req_cols_obj);

        auto df = ctx.csv(bts_path);
        auto cols = extractFromListOfStrings(df.columns().ptr());

        vector<string> renamed_cols;
        for (auto & col : cols) {
            renamed_cols.emplace_back(normalizeCol(col));
        }
        // rename columns
        for (int i = 0; i < cols.size(); ++i)
            df = df.renameColumn(cols[i], renamed_cols[i]);

        // caching active, so select cols required and cache!
        // and pushdown
        //df = df.selectColumns(time_req_cols);
        df = df.cache(true);

        // airport caching doesn't work, try to fix it??
        // airport_cols = ['ICAOCode', 'IATACode', 'AirportName', 'AirportCity', 'Country',
        //         'LatitudeDegrees', 'LatitudeMinutes', 'LatitudeSeconds', 'LatitudeDirection',
        //         'LongitudeDegrees', 'LongitudeMinutes', 'LongitudeSeconds',
        //         'LongitudeDirection', 'Altitude', 'LatitudeDecimal', 'LongitudeDecimal']
        //
        // df_airports = ctx.csv(airport_data_path, columns=airport_cols, delimiter=':', header=False, null_values=['', 'N/a', 'N/A'])
        // if args.simulate_spark:
        // df_airports = df_airports.cache()

        auto null_values_obj = python::runAndGet("NV=['', 'N/a', 'N/A']", "NV");
        auto airport_cols_obj = python::runAndGet("airport_cols = ['ICAOCode', 'IATACode', 'AirportName', 'AirportCity', 'Country',\n"
                                             "                'LatitudeDegrees', 'LatitudeMinutes', 'LatitudeSeconds', 'LatitudeDirection',\n"
                                             "                'LongitudeDegrees', 'LongitudeMinutes', 'LongitudeSeconds',\n"
                                             "                'LongitudeDirection', 'Altitude', 'LatitudeDecimal', 'LongitudeDecimal']", "airport_cols");
        auto null_values = py::reinterpret_borrow<py::list>(null_values_obj);
        auto airport_cols = py::reinterpret_borrow<py::list>(airport_cols_obj);
        auto df_airports = ctx.csv(airport_path, airport_cols, true, false, ":", "\"", null_values);
        df_airports = df_airports.cache(true);

        auto df_carrier = ctx.csv(carrier_path);
        df_carrier = df_carrier.cache(true);

        auto df_all = df.join(df_carrier, "OpUniqueCarrier", "Code", "", "", "", "");

        df_all = df_all.cache(true); // this may fail!
    }
}


TEST_F(WrapperTest, Airport) {
    using namespace tuplex;
    using namespace std;

    string sampleFile = "../resources/pipelines/flights/GlobalAirportDatabase.txt";

    // RAII, destruct python context!
    PythonContext c("python", "",
                    testOptions().asJSON());

    // execute mini part of pipeline and output csv to file
    // pipeline is
    // df = ctx.csv(perf_path)
    //
    //# rename weird column names from original file
    //renamed_cols = list(map(lambda c: ''.join(map(lambda w: w.capitalize(), c.split('_'))), df.columns))
    //
    //for i, c in enumerate(df.columns):
    //    df = df.renameColumn(c, renamed_cols[i])
    //
    //# split into city and state!
    //df = df.withColumn('OriginCity', lambda x: x['OriginCityName'][:x['OriginCityName'].rfind(',')].strip())
    //df = df.withColumn('OriginState', lambda x: x['OriginCityName'][x['OriginCityName'].rfind(',')+1:].strip())
    //
    //df = df.withColumn('DestCity', lambda x: x['DestCityName'][:x['DestCityName'].rfind(',')].strip())
    //df = df.withColumn('DestState', lambda x: x['DestCityName'][x['DestCityName'].rfind(',')+1:].strip())
    //
    //df.tocsv("tuplex_output")
    // weird block syntax due to RAII problems.
    {

        auto closureObject = PyDict_New();
        auto string_mod = PyImport_ImportModule("string");
        ASSERT_TRUE(string_mod);
        PyDict_SetItemString(closureObject, "string", string_mod);

        vector<string> airport_cols{"ICAOCode", "IATACode", "AirportName", "AirportCity", "Country", "LatitudeDegrees",
                                    "LatitudeMinutes",
                                    "LatitudeSeconds", "LatitudeDirection", "LongitudeDegrees", "LongitudeMinutes",
                                    "LongitudeSeconds", "LongitudeDirection", "Altitude", "LatitudeDecimal",
                                    "LongitudeDecimal"};
        auto pds = c.csv(sampleFile, STL_to_Python(airport_cols), false, false, ":");


        pds = pds.mapColumn("AirportName", "lambda x: string.capwords(x) if x else None", "", py::reinterpret_steal<py::dict>(closureObject));
        pds.tocsv("airport.csv");
    }
}

TEST_F(WrapperTest, OptionParallelizeI) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = PyList_New(5);
    PyList_SET_ITEM(listObj, 0, PyLong_FromLong(112));
    PyList_SET_ITEM(listObj, 1, Py_None);
    PyList_SET_ITEM(listObj, 2, PyLong_FromLong(213));
    PyList_SET_ITEM(listObj, 3, PyLong_FromLong(345));
    PyList_SET_ITEM(listObj, 4, Py_None);

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 5);

        // check contents
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 0)), Row(112));
        EXPECT_EQ(PyList_GetItem(resObj, 1), Py_None);
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 2)), Row(213));
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 3)), Row(345));
        EXPECT_EQ(PyList_GetItem(resObj, 4), Py_None);
    }
}

TEST_F(WrapperTest, OptionParallelizeII) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = PyList_New(5);

    PyObject * l1 = PyList_New(2);
    PyList_SET_ITEM(l1, 0, PyLong_FromLong(1));
    PyList_SET_ITEM(l1, 1, PyLong_FromLong(2));

    PyObject * l2 = PyList_New(2);
    PyList_SET_ITEM(l2, 0, PyLong_FromLong(3));
    PyList_SET_ITEM(l2, 1, PyLong_FromLong(4));

    PyObject * l3 = PyList_New(2);
    PyList_SET_ITEM(l3, 0, PyLong_FromLong(5));
    PyList_SET_ITEM(l3, 1, PyLong_FromLong(6));

    PyList_SET_ITEM(listObj, 0, l1);
    PyList_SET_ITEM(listObj, 1, Py_None);
    PyList_SET_ITEM(listObj, 2, l2);
    PyList_SET_ITEM(listObj, 3, l3);
    PyList_SET_ITEM(listObj, 4, Py_None);

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 5);

        // check contents
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 0)), Row(List(1, 2)));
        EXPECT_EQ(PyList_GetItem(resObj, 1), Py_None);
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 2)), Row(List(3, 4)));
        EXPECT_EQ(python::pythonToRow(PyList_GetItem(resObj, 3)), Row(List(5, 6)));
        EXPECT_EQ(PyList_GetItem(resObj, 4), Py_None);
    }
}

TEST_F(WrapperTest, NoneParallelize) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = PyList_New(2);
    PyList_SET_ITEM(listObj, 0, Py_None);
    PyList_SET_ITEM(listObj, 1, Py_None);

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 2);

        // check contents
        EXPECT_EQ(PyList_GetItem(resObj, 0), Py_None);
        EXPECT_EQ(PyList_GetItem(resObj, 1), Py_None);
    }
}

TEST_F(WrapperTest, EmptyMapI) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = PyList_New(4);
    PyList_SET_ITEM(listObj, 0, PyLong_FromLong(1));
    PyList_SET_ITEM(listObj, 1, PyLong_FromLong(2));
    PyList_SET_ITEM(listObj, 2, PyLong_FromLong(3));
    PyList_SET_ITEM(listObj, 3, PyLong_FromLong(4));

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).map("lambda x: None", "").collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 4);

        // check contents
        EXPECT_EQ(PyList_GetItem(resObj, 0), Py_None);
        EXPECT_EQ(PyList_GetItem(resObj, 1), Py_None);
        EXPECT_EQ(PyList_GetItem(resObj, 2), Py_None);
        EXPECT_EQ(PyList_GetItem(resObj, 3), Py_None);
    }
}

TEST_F(WrapperTest, EmptyMapII) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = PyList_New(4);
    PyList_SET_ITEM(listObj, 0, PyLong_FromLong(1));
    PyList_SET_ITEM(listObj, 1, PyLong_FromLong(2));
    PyList_SET_ITEM(listObj, 2, PyLong_FromLong(3));
    PyList_SET_ITEM(listObj, 3, PyLong_FromLong(4));

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).map("lambda x: ()", "").collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 4);

        // check contents (each one is an empty tuple)
        EXPECT_TRUE(PyTuple_Check(PyList_GetItem(resObj, 0)));
        EXPECT_EQ(PyTuple_GET_SIZE(PyList_GetItem(resObj, 0)), 0);
        EXPECT_TRUE(PyTuple_Check(PyList_GetItem(resObj, 1)));
        EXPECT_EQ(PyTuple_GET_SIZE(PyList_GetItem(resObj, 1)), 0);
        EXPECT_TRUE(PyTuple_Check(PyList_GetItem(resObj, 2)));
        EXPECT_EQ(PyTuple_GET_SIZE(PyList_GetItem(resObj, 2)), 0);
        EXPECT_TRUE(PyTuple_Check(PyList_GetItem(resObj, 3)));
        EXPECT_EQ(PyTuple_GET_SIZE(PyList_GetItem(resObj, 3)), 0);
    }
}

TEST_F(WrapperTest, EmptyMapIII) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = PyList_New(4);
    PyList_SET_ITEM(listObj, 0, PyLong_FromLong(1));
    PyList_SET_ITEM(listObj, 1, PyLong_FromLong(2));
    PyList_SET_ITEM(listObj, 2, PyLong_FromLong(3));
    PyList_SET_ITEM(listObj, 3, PyLong_FromLong(4));

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).map("lambda x: {}", "").collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 4);

        // check contents (each one is an empty tuple)
        EXPECT_TRUE(PyDict_Check(PyList_GetItem(resObj, 0)));
        EXPECT_EQ(PyDict_Size(PyList_GetItem(resObj, 0)), 0);
        EXPECT_TRUE(PyDict_Check(PyList_GetItem(resObj, 1)));
        EXPECT_EQ(PyDict_Size(PyList_GetItem(resObj, 1)), 0);
        EXPECT_TRUE(PyDict_Check(PyList_GetItem(resObj, 2)));
        EXPECT_EQ(PyDict_Size(PyList_GetItem(resObj, 2)), 0);
        EXPECT_TRUE(PyDict_Check(PyList_GetItem(resObj, 3)));
        EXPECT_EQ(PyDict_Size(PyList_GetItem(resObj, 3)), 0);
    }
}

TEST_F(WrapperTest, EmptyOptionMapI) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = PyList_New(4);
    PyList_SET_ITEM(listObj, 0, PyLong_FromLong(1));
    PyList_SET_ITEM(listObj, 1, PyLong_FromLong(2));
    PyList_SET_ITEM(listObj, 2, PyLong_FromLong(3));
    PyList_SET_ITEM(listObj, 3, PyLong_FromLong(4));

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).map("lambda x: () if x < 3 else None", "").collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 4);

        // check contents (each one is an empty tuple)
        EXPECT_TRUE(PyTuple_Check(PyList_GetItem(resObj, 0)));
        EXPECT_EQ(PyTuple_GET_SIZE(PyList_GetItem(resObj, 0)), 0);
        EXPECT_TRUE(PyTuple_Check(PyList_GetItem(resObj, 1)));
        EXPECT_EQ(PyTuple_GET_SIZE(PyList_GetItem(resObj, 1)), 0);
        EXPECT_EQ(PyList_GetItem(resObj, 2), Py_None);
        EXPECT_EQ(PyList_GetItem(resObj, 3), Py_None);
    }
}

TEST_F(WrapperTest, EmptyOptionMapII) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = PyList_New(4);
    PyList_SET_ITEM(listObj, 0, PyLong_FromLong(1));
    PyList_SET_ITEM(listObj, 1, PyLong_FromLong(2));
    PyList_SET_ITEM(listObj, 2, PyLong_FromLong(3));
    PyList_SET_ITEM(listObj, 3, PyLong_FromLong(4));

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).map("lambda x: {} if x < 3 else None", "").collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 4);

        // check contents (each one is an empty tuple)
        EXPECT_TRUE(PyDict_Check(PyList_GetItem(resObj, 0)));
        EXPECT_EQ(PyDict_Size(PyList_GetItem(resObj, 0)), 0);
        EXPECT_TRUE(PyDict_Check(PyList_GetItem(resObj, 1)));
        EXPECT_EQ(PyDict_Size(PyList_GetItem(resObj, 1)), 0);
        EXPECT_EQ(PyList_GetItem(resObj, 2), Py_None);
        EXPECT_EQ(PyList_GetItem(resObj, 3), Py_None);
    }
}

TEST_F(WrapperTest, OptionTupleParallelizeI) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = PyList_New(3);

    PyObject * t1 = PyTuple_New(2);
    PyTuple_SET_ITEM(t1, 0, PyLong_FromLong(11));
    PyTuple_SET_ITEM(t1, 1, PyLong_FromLong(12));

    PyObject * t2 = PyTuple_New(2);
    PyTuple_SET_ITEM(t2, 0, Py_None);
    PyTuple_SET_ITEM(t2, 1, PyLong_FromLong(100));

    PyObject * t3 = PyTuple_New(2);
    PyTuple_SET_ITEM(t3, 0, PyLong_FromLong(200));
    PyTuple_SET_ITEM(t3, 1, Py_None);

    PyList_SET_ITEM(listObj, 0, t1);
    PyList_SET_ITEM(listObj, 1, t2);
    PyList_SET_ITEM(listObj, 2, t3);

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 3);

        // check contents
        auto el1 = PyList_GetItem(resObj, 0);
        auto el2 = PyList_GetItem(resObj, 1);
        auto el3 = PyList_GetItem(resObj, 2);
        ASSERT_TRUE(PyTuple_Check(el1));
        ASSERT_TRUE(PyTuple_Check(el2));
        ASSERT_TRUE(PyTuple_Check(el3));

        EXPECT_EQ(python::pythonToRow(PyTuple_GetItem(el1, 0)), Row(11));
        EXPECT_EQ(python::pythonToRow(PyTuple_GetItem(el1, 1)), Row(12));
        EXPECT_EQ(PyTuple_GetItem(el2, 0), Py_None);
        EXPECT_EQ(python::pythonToRow(PyTuple_GetItem(el2, 1)), Row(100));
        EXPECT_EQ(python::pythonToRow(PyTuple_GetItem(el3, 0)), Row(200));
        EXPECT_EQ(PyTuple_GetItem(el3, 1), Py_None);
    }
}

TEST_F(WrapperTest, OptionTupleParallelizeII) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = PyList_New(3);

    PyObject * t1 = PyTuple_New(2);
    PyTuple_SET_ITEM(t1, 0, PyLong_FromLong(11));
    PyTuple_SET_ITEM(t1, 1, PyFloat_FromDouble(12.1));

    PyObject * t2 = PyTuple_New(2);
    PyTuple_SET_ITEM(t2, 0, Py_None);
    PyTuple_SET_ITEM(t2, 1, PyFloat_FromDouble(100.1));

    PyObject * t3 = PyTuple_New(2);
    PyTuple_SET_ITEM(t3, 0, PyLong_FromLong(200));
    PyTuple_SET_ITEM(t3, 1, Py_None);

    PyList_SET_ITEM(listObj, 0, t1);
    PyList_SET_ITEM(listObj, 1, t2);
    PyList_SET_ITEM(listObj, 2, t3);

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 3);

        // check contents
        auto el1 = PyList_GetItem(resObj, 0);
        auto el2 = PyList_GetItem(resObj, 1);
        auto el3 = PyList_GetItem(resObj, 2);
        ASSERT_TRUE(PyTuple_Check(el1));
        ASSERT_TRUE(PyTuple_Check(el2));
        ASSERT_TRUE(PyTuple_Check(el3));

        EXPECT_EQ(python::pythonToRow(PyTuple_GetItem(el1, 0)), Row(11));
        EXPECT_EQ(python::pythonToRow(PyTuple_GetItem(el1, 1)), Row(12.1));
        EXPECT_EQ(PyTuple_GetItem(el2, 0), Py_None);
        EXPECT_EQ(python::pythonToRow(PyTuple_GetItem(el2, 1)), Row(100.1));
        EXPECT_EQ(python::pythonToRow(PyTuple_GetItem(el3, 0)), Row(200));
        EXPECT_EQ(PyTuple_GetItem(el3, 1), Py_None);
    }
}

TEST_F(WrapperTest, OptionTupleParallelizeIII) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = PyList_New(3);

    PyObject * t1 = PyTuple_New(2);
    PyTuple_SET_ITEM(t1, 0, PyFloat_FromDouble(11.1));
    PyTuple_SET_ITEM(t1, 1, python::PyString_FromString("Hello123"));

    PyObject * t2 = PyTuple_New(2);
    PyTuple_SET_ITEM(t2, 0, Py_None);
    PyTuple_SET_ITEM(t2, 1, python::PyString_FromString("Hello12"));

    PyObject * t3 = PyTuple_New(2);
    PyTuple_SET_ITEM(t3, 0, PyFloat_FromDouble(200.1));
    PyTuple_SET_ITEM(t3, 1, python::PyString_FromString("Hello1"));

    PyList_SET_ITEM(listObj, 0, t1);
    PyList_SET_ITEM(listObj, 1, t2);
    PyList_SET_ITEM(listObj, 2, t3);

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).collect();
        auto resObj = res.ptr();

        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_GET_SIZE(resObj), 3);

        // check contents
        auto el1 = PyList_GetItem(resObj, 0);
        auto el2 = PyList_GetItem(resObj, 1);
        auto el3 = PyList_GetItem(resObj, 2);
        ASSERT_TRUE(PyTuple_Check(el1));
        ASSERT_TRUE(PyTuple_Check(el2));
        ASSERT_TRUE(PyTuple_Check(el3));

        EXPECT_EQ(python::pythonToRow(PyTuple_GetItem(el1, 0)), Row(11.1));
        EXPECT_EQ(python::pythonToRow(PyTuple_GetItem(el1, 1)), Row("Hello123"));
        EXPECT_EQ(PyTuple_GetItem(el2, 0), Py_None);
        EXPECT_EQ(python::pythonToRow(PyTuple_GetItem(el2, 1)), Row("Hello12"));
        EXPECT_EQ(python::pythonToRow(PyTuple_GetItem(el3, 0)), Row(200.1));
        EXPECT_EQ(python::pythonToRow(PyTuple_GetItem(el3, 1)), Row("Hello1"));
    }
}

TEST_F(WrapperTest, parallelizeOptionTypeI) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = python::runAndGet(
            "test_input = [(1.0, '2', 3, '4', 5, 6, True, 8, 9, None), (None, '2', 3, None, 5, 6, True, 8, 9, None)"
            ", (1.0, '2', 3, '4', None, 6, None, 8, 9, None)]",
            "test_input");

    Py_XINCREF(listObj); // for str conversion
    ASSERT_TRUE(listObj);

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).collect();
        auto resObj = res.ptr();

        // simplest is string compare
        auto ref_str = python::PyString_AsString(listObj);
        auto res_str = python::PyString_AsString(resObj);

        EXPECT_EQ(ref_str, res_str);
    }
}

TEST_F(WrapperTest, parallelizeNestedSlice) {
    using namespace tuplex;

    PythonContext c("c", "", microTestOptions().asJSON());

    PyObject * listObj = python::runAndGet(
            "test_input = [((), (\"hello\",), 123, \"oh no\", (1, 2)), ((), (\"goodbye\",), 123, \"yes\", (-10, 2)),\n"
            "                     ((), (\"foobar\",), 1443, \"no\", (100, 0))]",
            "test_input");

    Py_XINCREF(listObj); // for str conversion
    ASSERT_TRUE(listObj);

    // weird block syntax due to RAII problems.
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).map("lambda x: x[-10:]", "").collect();
        auto resObj = res.ptr();

        // simplest is string compare
        auto ref_str = python::PyString_AsString(listObj);
        auto res_str = python::PyString_AsString(resObj);

        EXPECT_EQ(ref_str, res_str);
    }
}

TEST_F(WrapperTest, TPCHQ6) {
    using namespace tuplex;

    PyObject * listObj = python::runAndGet("listitem_columns = ['l_orderkey', 'l_partkey', 'l_suppkey',\n"
                                           "                    'l_linenumber', 'l_quantity', 'l_extendedprice',\n"
                                           "                    'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus',\n"
                                           "                    'l_shipdate', 'l_commitdate', 'l_receiptdate',\n"
                                           "                    'l_shipinstruct', 'l_shipmode', 'l_comment']", "listitem_columns");
    PythonContext c("c", "", testOptions().asJSON());

    {

        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto init_val = python::pickleObject(python::getMainModule(), PyFloat_FromDouble(0.0));

        auto path = "../resources/tpch/lineitem.tbl"; // SF 0.01
        c.csv(path, list, false, false, "|").mapColumn("l_shipdate", "lambda x: int(x.replace('-', ''))", "")
        .filter("lambda x: 19940101 <= x['l_shipdate'] < 19940101 + 10000", "")
        .filter("lambda x: 0.06 - 0.01 <= x['l_discount'] <= 0.06 + 0.01", "")
        .filter("lambda x: x['l_quantity'] < 24", "")
        .aggregate("lambda a, b: a + b", "", "lambda a, x: a + x[5] * x[6]", "", init_val)
        .show();
    }
}

TEST_F(WrapperTest, TupleParallelizeI) {
    using namespace tuplex;

    PyObject* listObj = python::runAndGet("L = [('hello', 'world', 'hi', 1, 2, 3), ('foo', 'bar', 'baz', 4, 5, 6), ('blank', '', 'not', 7, 8, 9)]", "L");

    PythonContext c("c", "", microTestOptions().asJSON());
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        c.parallelize(list).map("lambda x: ({x[0]: x[3], x[1]: x[4], x[2]: x[5]},)", "").show();
    }
}

TEST_F(WrapperTest, TupleParallelizeII) {
    using namespace tuplex;

    PyObject* listObj = python::runAndGet("L = [({}, {}, {}), ({}, {}, {}), ({}, {}, {})]", "L");

    PythonContext c("c", "", microTestOptions().asJSON());
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        c.parallelize(list).map("lambda x, y, z: [x, y, z]", "").show();
    }
}

// On Mac OS X, an error gets produced when calling following things:
// py.test -s -k 'test_combined' -vvv
TEST_F(WrapperTest, DictParallelizeRefTest) {
    using namespace tuplex;
    using namespace std;


    PyObject* strings = python::runAndGet("strings = [('hello', 'world', 'hi'), ('foo', 'bar', 'baz'), ('blank', '', 'not')]\n", "strings");
    PyObject* floats = python::runAndGet("floats = [(1.2, 3.4, -100.2), (5.6, 7.8, -1.234), (9.0, 0.1, 2.3)]\n", "floats");
    ASSERT_TRUE(floats->ob_refcnt > 0);
    PythonContext c("c", "", microTestOptions().asJSON());

    {

        auto L = python::runAndGet("floats = [(1.2, 3.4, -100.2), (5.6, 7.8, -1.234), (9.0, 0.1, 2.3)]\n"
                                   "# for dictionaries with string keys, the first level gets auto-unpacked.\n"
                                   "l3 = [{'a' : x[0], 'b' : x[1], 'c': x[2]} for x in floats]", "l3");

        ASSERT_TRUE(floats->ob_refcnt > 0);
        ASSERT_EQ(PyList_Size(floats), 3);
        ASSERT_TRUE(floats->ob_refcnt > 0);

        // following code screws up python objects!
        auto list = py::reinterpret_borrow<py::list>(L);
        ASSERT_EQ(py::len(list), 3);
        ASSERT_EQ(PyList_Size(floats), 3);
        ASSERT_EQ(PyObject_Length(floats), 3);
        auto ds = c.parallelize(list).map("lambda x: (x['a'], x['b'], x['c'])", "");
        ASSERT_TRUE(floats->ob_refcnt > 0);

        // ANTLR4 causing this??
        ASSERT_EQ(PyList_Size(floats), 3);
        ASSERT_EQ(PyObject_Length(floats), 3);

        auto v = ds.collect();

        ASSERT_EQ(PyObject_Length(floats), 3);

        PyObject_Print(v.ptr(), stdout, 0);
        cout<<endl;

        auto flaotslen = PyObject_Length(floats);
        auto vlen = PyObject_Length(v.ptr());

        // compare
        ASSERT_EQ(py::len(v), PyList_Size(floats));
    }
}


TEST_F(WrapperTest, BuiltinModule) {
    using namespace tuplex;
    using namespace std;
    PythonContext c("c", "", microTestOptions().asJSON());

    {
        PyObject* L = PyList_New(3);
        PyList_SetItem(L, 0, python::PyString_FromString("123"));
        PyList_SetItem(L, 1, python::PyString_FromString("abc"));
        PyList_SetItem(L, 2, python::PyString_FromString("1a2b3c"));
        auto list = py::reinterpret_borrow<py::list>(L);

        PyObject* closureObject = PyDict_New();
        // import re module
        auto re_mod = PyImport_ImportModule("re");
        PyDict_SetItemString(closureObject, "re", re_mod);
        auto v = c.parallelize(list).map("lambda x: re.search('\\\\d+', x) != None", "", py::reinterpret_steal<py::dict>(closureObject)).collect();

        ASSERT_EQ(PyObject_Length(v.ptr()), 3);
        auto v_str = python::PyString_AsString(v.ptr());
        EXPECT_EQ(v_str, "[True, False, True]");
    }
}

TEST_F(WrapperTest, SwapIII) {
    using namespace tuplex;
    using namespace std;

    auto code = "def swapIII(x):\n"
                "    a = x[0]\n"
                "    b = x[1]\n"
                "    b, a = a, b\n"
                "    return a, b\n"
                "\n";

    PythonContext c("c", "", microTestOptions().asJSON());
    {
        PyObject* L = PyList_New(2);
        auto tuple1 = PyTuple_New(2);
        PyTuple_SetItem(tuple1, 0, python::PyString_FromString("a"));
        PyTuple_SetItem(tuple1, 1, PyLong_FromLong(1));
        auto tuple2 = PyTuple_New(2);
        PyTuple_SetItem(tuple2, 0, python::PyString_FromString("b"));
        PyTuple_SetItem(tuple2, 1, PyLong_FromLong(2));
        PyList_SetItem(L, 0, tuple1);
        PyList_SetItem(L, 1, tuple2);
        auto list = py::reinterpret_borrow<py::list>(L);

        auto v = c.parallelize(list).map(code, "").collect();
        ASSERT_EQ(PyObject_Length(v.ptr()), 2);
        auto v_str = python::PyString_AsString(v.ptr());
        EXPECT_EQ(v_str, "[(1, 'a'), (2, 'b')]"); // python formatting...

        // compare pure python version
        ExceptionCode ec;
        auto py_func = python::compileFunction(python::getMainModule(), code);
        auto pcr_1 = python::callFunction(py_func, python::rowToPython(Row("a", 1)), ec);
        ASSERT_EQ(ec, ExceptionCode::SUCCESS);
        ASSERT_TRUE(pcr_1);
        auto pcr_2 = python::callFunction(py_func, python::rowToPython(Row("b", 2)), ec);
        ASSERT_EQ(ec, ExceptionCode::SUCCESS);
        ASSERT_TRUE(pcr_2);

        auto res_1 = python::PyString_AsString(pcr_1);
        auto res_2 = python::PyString_AsString(pcr_2);

        EXPECT_EQ(v_str, "[" + res_1 + ", " + res_2 + "]");
    }
}

namespace tuplex {

    static auto extractBd_c = "def extractBd(x):\n"
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

    static auto extractBdAltLogic_c = "def extractBd(x):\n"
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
                               "   m = re.search(r'^\\d+$', r)\n"
                               "   if m:\n"
                               "       return int(r)\n"
                               "   else:\n"
                               "       return None";

    static auto extractBa_c = "def extractBa(x):\n"
                       "    val = x['facts and features']\n"
                       "    max_idx = val.find(' ba')\n"
                       "    if max_idx < 0:\n"
                       "        max_idx = len(val)\n"
                       "    s = val[:max_idx]\n"
                       "    # find comma before\n"
                       "    split_idx = s.rfind(',')\n"
                       "    if split_idx < 0:\n"
                       "        split_idx = 0\n"
                       "    else:\n"
                       "        split_idx += 2\n"
                       "    r = s[split_idx:]\n"
                       "    return math.ceil(2.0 * float(r)) / 2.0\n";
    static auto extractSqft_c = "def extractSqft(x):\n"
                         "    val = x['facts and features']\n"
                         "    max_idx = val.find(' sqft')\n"
                         "    if max_idx < 0:\n"
                         "        max_idx = len(val)\n"
                         "    s = val[:max_idx]\n"
                         "    split_idx = s.rfind('ba ,')\n"
                         "    if split_idx < 0:\n"
                         "        split_idx = 0\n"
                         "    else:\n"
                         "        split_idx += 5\n"
                         "    r = s[split_idx:]\n"
                         "    r = r.replace(',', '')\n"
                         "    return int(r)";
    static auto extractOffer_c = "def extractOffer(x):\n"
                          "    offer = x['title'].lower()\n"
                          "    if 'sale' in offer:\n"
                          "        return 'sale'\n"
                          "    if 'rent' in offer:\n"
                          "        return 'rent'\n"
                          "    if 'sold' in offer:\n"
                          "        return 'sold'\n"
                          "    if 'foreclose' in offer.lower():\n"
                          "        return 'foreclosed'\n"
                          "    return offer";
    static auto extractType_c = "def extractType(x):\n"
                         "    t = x['title'].lower()\n"
                         "    type = 'unknown'\n"
                         "    if 'condo' in t or 'apartment' in t:\n"
                         "        type = 'condo'\n"
                         "    if 'house' in t:\n"
                         "        type = 'house'\n"
                         "    return type";
    static auto extractPrice_c = "def extractPrice(x):\n"
                          "    price = x['price']\n"
                          "    p = 0\n"
                          "    if x['offer'] == 'sold':\n"
                          "        # price is to be calculated using price/sqft * sqft\n"
                          "        val = x['facts and features']\n"
                          "        s = val[val.find('Price/sqft:') + len('Price/sqft:') + 1:]\n"
                          "        r = s[s.find('$')+1:s.find(', ') - 1]\n"
                          "        price_per_sqft = int(r)\n"
                          "        p = price_per_sqft * x['sqft']\n"
                          "    elif x['offer'] == 'rent':\n"
                          "        max_idx = price.rfind('/')\n"
                          "        p = int(price[1:max_idx].replace(',', ''))\n"
                          "    else:\n"
                          "        # take price from price column\n"
                          "        p = int(price[1:].replace(',', ''))\n"
                          "    return p";

    static auto resolveBd_c = "def resolveBd(x):\n"
                       "    if 'Studio' in x['facts and features']:\n"
                       "        return 1\n"
                       "    raise ValueError\n";

    static auto resolveBa_c = "def extractBa(x):\n"
                       "    val = x['facts and features']\n"
                       "    max_idx = val.find(' ba')\n"
                       "    if max_idx < 0:\n"
                       "        max_idx = len(val)\n"
                       "    s = val[:max_idx]\n"
                       "    # find comma before\n"
                       "    split_idx = s.rfind(',')\n"
                       "    if split_idx < 0:\n"
                       "        split_idx = 0\n"
                       "    else:\n"
                       "        split_idx += 2\n"
                       "    r = s[split_idx:]\n"
                       "    return math.ceil(float(r))";

    static auto extractZipcodeAltLogic_c = "def extractZipcode(x):\n"
                                            "    m = re.search(r'^\\d+$', x['postal_code'])\n"
                                            "    if m:\n"
                                            "        return '%05d' % int(x['postal_code'])\n"
                                            "    else:\n"
                                            "        return None";


    PythonDataSet zillowDirtyNoResolvers(PythonContext& ctx, const std::string& z_path) {

        // create closure object for resolve_Ba
        auto ba_closure = PyDict_New();
        auto math_mod = PyImport_ImportModule("math");
        auto re_mod = PyImport_ImportModule("re");
        assert(math_mod); assert(re_mod);
        PyDict_SetItemString(ba_closure, "math", math_mod);
        PyDict_SetItemString(ba_closure, "re", re_mod);

        auto cols_to_select = python::runAndGet("L = ['url', 'zipcode', 'address', 'city', 'state',"
                                                "'bedrooms', 'bathrooms', 'sqft', 'offer', 'type', 'price']", "L");

        return ctx.csv(z_path)
           .withColumn("bedrooms", extractBd_c, "")
           .filter("lambda x: x['bedrooms'] < 10", "")
           .withColumn("type", extractType_c, "")
           .filter("lambda x: x['type'] == 'house'", "")
           .withColumn("zipcode", "lambda x: '%05d' % int(x['postal_code'])", "")
           .mapColumn("city", "lambda x: x[0].upper() + x[1:].lower()", "")
           .withColumn("bathrooms", extractBa_c, "", py::reinterpret_steal<py::dict>(ba_closure))
           .withColumn("sqft", extractSqft_c, "")
           .withColumn("offer", extractOffer_c, "")
           .withColumn("price", extractPrice_c, "")
           .filter("lambda x: 100000 < x['price'] < 2e7", "")
           .selectColumns(py::reinterpret_borrow<py::list>(cols_to_select));
    }

    PythonDataSet zillowDirtyWithResolvers(PythonContext& ctx, const std::string& z_path) {

        // create closure object for resolve_Ba
        auto ba_closure = PyDict_New();
        auto math_mod = PyImport_ImportModule("math");
        auto re_mod = PyImport_ImportModule("re");
        assert(math_mod); assert(re_mod);
        PyDict_SetItemString(ba_closure, "math", math_mod);
        PyDict_SetItemString(ba_closure, "re", re_mod);

        auto cols_to_select = python::runAndGet("L = ['url', 'zipcode', 'address', 'city', 'state',"
                                                "'bedrooms', 'bathrooms', 'sqft', 'offer', 'type', 'price']", "L");

        return ctx.csv(z_path)
                .withColumn("bedrooms", extractBd_c, "")
                .resolve(ecToI64(ExceptionCode::VALUEERROR), resolveBd_c, "")
                .ignore(ecToI64(ExceptionCode::VALUEERROR))
                .filter("lambda x: x['bedrooms'] < 10", "")
                .withColumn("type", extractType_c, "")
                .filter("lambda x: x['type'] == 'house'", "")
                .withColumn("zipcode", "lambda x: '%05d' % int(x['postal_code'])", "")
                .ignore(ecToI64(ExceptionCode::TYPEERROR))
                .mapColumn("city", "lambda x: x[0].upper() + x[1:].lower()", "")
                .withColumn("bathrooms", extractBa_c, "", py::reinterpret_steal<py::dict>(ba_closure))
                .ignore(ecToI64(ExceptionCode::VALUEERROR))
                .withColumn("sqft", extractSqft_c, "")
                .ignore(ecToI64(ExceptionCode::VALUEERROR)) // why is this showing a single error???
                .withColumn("offer", extractOffer_c, "")
                .withColumn("price", extractPrice_c, "")
                .filter("lambda x: 100000 < x['price'] < 2e7", "")
                .selectColumns(py::reinterpret_borrow<py::list>(cols_to_select));
    }

    PythonDataSet zillowDirtyCustomLogic(PythonContext& ctx, const std::string& z_path) {

        // create closure object for resolve_Ba
        auto ba_closure = PyDict_New();
        auto math_mod = PyImport_ImportModule("math");
        auto re_mod = PyImport_ImportModule("re");
        assert(math_mod); assert(re_mod);
        PyDict_SetItemString(ba_closure, "math", math_mod);
        PyDict_SetItemString(ba_closure, "re", re_mod);

        auto cols_to_select = python::runAndGet("L = ['url', 'zipcode', 'address', 'city', 'state',"
                                                "'bedrooms', 'bathrooms', 'sqft', 'offer', 'type', 'price']", "L");


        auto extractBd_alt_c = "def extractBd(x):\n"
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
                               "   # regex check whether we can cast\n"
                               "   m = re.search(r'^\\d+$', r)\n"
                               "   if m:\n"
                               "       return int(r)\n"
                               "   else:\n"
                               "       return None";

        auto extractBa_alt_c = "def extractBa(x):\n"
                               "    val = x['facts and features']\n"
                               "    max_idx = val.find(' ba')\n"
                               "    if max_idx < 0:\n"
                               "        max_idx = len(val)\n"
                               "    s = val[:max_idx]\n"
                               "    # find comma before\n"
                               "    split_idx = s.rfind(',')\n"
                               "    if split_idx < 0:\n"
                               "        split_idx = 0\n"
                               "    else:\n"
                               "        split_idx += 2\n"
                               "    r = s[split_idx:]\n"
                               "    # check if valid float string by using regex matching\n"
                               "    m = re.search(r'^\\d+.?\\d*$', r)\n"
                               "    if m:\n"
                               "        ba = math.ceil(2.0 * float(r)) / 2.0\n"
                               "        return ba\n"
                               "    else:\n"
                               "        return None";

        auto extractSqft_alt_c = "def extractSqft(x):\n"
                                 "    val = x['facts and features']\n"
                                 "    max_idx = val.find(' sqft')\n"
                                 "    if max_idx < 0:\n"
                                 "        max_idx = len(val)\n"
                                 "    s = val[:max_idx]\n"
                                 "    split_idx = s.rfind('ba ,')\n"
                                 "    if split_idx < 0:\n"
                                 "        split_idx = 0\n"
                                 "    else:\n"
                                 "        split_idx += 5\n"
                                 "    r = s[split_idx:]\n"
                                 "    r = r.replace(',', '')\n"
                                 "    m = re.search(r'^\\d+$', r)\n"
                                 "    if m:\n"
                                 "        return int(r)\n"
                                 "    else:\n"
                                 "        return None";

        return ctx.csv(z_path)
                  .withColumn("bedrooms", extractBd_alt_c, "", py::reinterpret_borrow<py::dict>(ba_closure))
                  .filter("lambda x: x['bedrooms'] != None and x['bedrooms'] < 10", "")
                  .withColumn("type", extractType_c, "", py::reinterpret_borrow<py::dict>(ba_closure))
                  .filter("lambda x: x['type'] == 'house'", "")
                  .filter("lambda x: x['postal_code'] != None", "")
                  .withColumn("zipcode", "lambda x: '%05d' % int(x['postal_code'])", "")
                  .filter("lambda x: x['zipcode'] != None", "")
                  .mapColumn("zipcode", "lambda x: str(x)", "")
                  .mapColumn("city", "lambda x: x[0].upper() + x[1:].lower()", "")
                  .withColumn("bathrooms", extractBa_alt_c, "", py::reinterpret_borrow<py::dict>(ba_closure))
                  .filter("lambda x: x['bathrooms'] != None", "")
                  .mapColumn("bathrooms", "lambda x: float(x)", "")
                  .withColumn("sqft", extractSqft_alt_c, "", py::reinterpret_borrow<py::dict>(ba_closure))
                  .filter("lambda x: x['sqft'] != None", "")
                  .mapColumn("sqft", "lambda x: int(x)", "")
                  .withColumn("offer", extractOffer_c, "", py::reinterpret_borrow<py::dict>(ba_closure))
                  .withColumn("price", extractPrice_c, "", py::reinterpret_borrow<py::dict>(ba_closure))
                  .filter("lambda x: 100000 < x['price'] < 2e7", "")
                .selectColumns(py::reinterpret_borrow<py::list>(cols_to_select));
    }
}

TEST_F(WrapperTest, ZillowDirty) {
    using namespace tuplex;
    using namespace std;
    string z_path = "../resources/zillow_dirty.csv";

    std::string output_path = "zillow_cleaned";

    // Note: when parsing dirty zillow with compiled parser, some lines do not get parsed correctly.
    // if compiled parser is disabled, it works.
    // need to fix why that is the case.

    auto json_opts = "{\"webui.enable\": false,"
                     " \"executorCount\": 6,"
                     " \"executorMemory\": \"64MB\","
                     " \"driverMemory\": \"64MB\","
                     " \"partitionSize\": \"4MB\","
                     " \"runTimeMemory\": \"4MB\","
                     " \"useLLVMOptimizer\": true,"
                     " \"optimizer.nullValueOptimization\": false,"
                     " \"csv.selectionPushdown\": false, "
                     "\"optimizer.generateParser\": false,"
                     "\"tuplex.scratchDir\": \"file://" + scratchDir + "\","
                     "\"optimizer.mergeExceptionsInOrder\": true}";

    auto json_opts_alt = "{\"webui.enable\": false,"
                     " \"executorCount\": 6,"
                     " \"executorMemory\": \"64MB\","
                     " \"driverMemory\": \"64MB\","
                     " \"partitionSize\": \"4MB\","
                     " \"runTimeMemory\": \"4MB\","
                     " \"useLLVMOptimizer\": true,"
                     " \"optimizer.nullValueOptimization\": false,"
                     " \"csv.selectionPushdown\": false, "
                     "\"tuplex.scratchDir\": \"file://" + scratchDir + "\","
                     "\"optimizer.generateParser\": false,"

                     "\"optimizer.mergeExceptionsInOrder\": false}";

    z_path = "../resources/zillow_dirty.csv";

    PythonContext ctx("", "", json_opts);
    PythonContext ctx_alt("", "", json_opts_alt);
    {

        // version I: no ignores/no resolvers
        // run to make sure this works
        zillowDirtyNoResolvers(ctx, z_path).tocsv(output_path);

        // version II: with ignores/resolvers
        // check that version with resolvers works
        zillowDirtyWithResolvers(ctx, z_path).tocsv(output_path); // this one writes out of bounds...

        // read tail of file and compare it to solution!
        // last rows is: https://www.zillow.com/homedetails/16-Rockmont-Rd-Belmont-MA-02478/56412009_zpid/,02478,16 Rockmont Rd,Belmont,MA,6,7,7947,sale,house,4200000
        auto result_file_content = fileToString(output_path + "/part0.csv");
        ASSERT_GT(result_file_content.size(), 0);
        auto needle = std::string("6,7.00000000,7947,sale,house,4200000\n");
        auto last_chars =  result_file_content.substr(result_file_content.size() - needle.size());
        EXPECT_EQ(last_chars, needle);

        // check that both in order and out-of-order merge have the same number of result rows
         auto in_order_rows_res = zillowDirtyWithResolvers(ctx, z_path).collect().ptr();
        auto out_of_order_rows_res = zillowDirtyWithResolvers(ctx_alt, z_path).collect().ptr();

        // both should be list
        ASSERT_TRUE(PyList_Check(in_order_rows_res));
        ASSERT_TRUE(PyList_Check(out_of_order_rows_res));
        EXPECT_EQ(PyList_Size(in_order_rows_res), PyList_Size(out_of_order_rows_res));

        // version III: with logic incorporated
        zillowDirtyCustomLogic(ctx, z_path).tocsv(output_path);
        // fetch tail and compare too
        result_file_content = fileToString(output_path + "/part0.csv");
        ASSERT_GT(result_file_content.size(), 0);
        last_chars =  result_file_content.substr(result_file_content.size() - needle.size());
        EXPECT_EQ(last_chars, needle);
    }
}

TEST_F(WrapperTest, BitwiseAnd) {
    // res = c.parallelize([(False, False), (False, True),
    //                             (True, False), (True, True)]).map(lambda a, b: a & b).collect()

    using namespace tuplex;

    PyObject* listObj = python::runAndGet("L = [(False, False), (False, True), (True, False), (True, True)]", "L");

    PythonContext c("c", "", microTestOptions().asJSON());
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res_list = c.parallelize(list).map("lambda a, b: a & b", "").collect();
        auto res_obj = res_list.ptr();
        ASSERT_TRUE(res_obj);
        ASSERT_TRUE(PyList_Check(res_obj));
        ASSERT_EQ(PyList_Size(res_obj), 4);
    }
}

TEST_F(WrapperTest, MetricsTest) {
    using namespace tuplex;

    PyObject* listObj = python::runAndGet("L = [(False, False), (False, True), (True, False), (True, True)]", "L");

    PythonContext c("c", "", microTestOptions().asJSON());
    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res_list = c.parallelize(list).map("lambda a, b: a & b", "").collect();
        auto res_obj = res_list.ptr();
        ASSERT_TRUE(res_obj);
        ASSERT_TRUE(PyList_Check(res_obj));
        ASSERT_EQ(PyList_Size(res_obj), 4);
    }

    // optimization time in LLVM depends on whether optimizer is activated or not.
    auto optimizer_enabled = py::cast<bool>(c.options()["tuplex.useLLVMOptimizer"]);
    PythonMetrics pythMet = c.getMetrics();
    auto pt1 = pythMet.getTotalCompilationTime();
    auto pt2 = pythMet.getLogicalOptimizationTime();
    auto pt3 = pythMet.getLLVMOptimizationTime();
    auto pt4 = pythMet.getLLVMCompilationTime();
    EXPECT_GT(pt1, 0.0);
    EXPECT_GT(pt2, 0.0);
    if(!optimizer_enabled)
        EXPECT_EQ(pt3, 0.0);
    else
        EXPECT_GT(pt3, 0.0);
    EXPECT_GT(pt4, 0.0);
}

TEST_F(WrapperTest, LambdaResolveI) {
    //         self.conf = {"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"}
    //        self.c = tuplex.Context(self.conf)
    //
    //    def test_LambdaResolveI(self):
    //
    //        ds = self.c.parallelize([0, 1, 2, 3, 4]).map(lambda x: 1. / x)
    //
    //        self.assertEqual(ds.collect(), [1. / 1, 1. / 2, 1. / 3, 1. / 4])
    //
    //        self.assertEqual(ds.resolve(ZeroDivisionError, lambda x: 42).collect(),
    //                         [42, 1. / 1, 1. / 2, 1. / 3, 1. / 4])
    using namespace tuplex;
    using namespace std;

    auto ctx_opts = "{\"webui.enable\": false,"
                         " \"driverMemory\": \"8MB\","
                         " \"partitionSize\": \"256KB\","
                         "\"tuplex.scratchDir\": \"file://" + scratchDir + "\","
                         "\"resolveWithInterpreterOnly\": true}";
    PythonContext ctx("", "", ctx_opts);


    auto L = python::runAndGet("L = [0, 1, 2, 3, 4]", "L");
    ASSERT_TRUE(L);
    Py_XINCREF(L);
    auto object_list = py::reinterpret_borrow<py::list>(L);

    {
//        // first try
//        auto res1 = ctx.parallelize(object_list).map("lambda x: 1. / x", "").collect();
//        ASSERT_TRUE(res1.ptr());
//        ASSERT_TRUE(PyList_Check(res1.ptr()));
//        EXPECT_EQ(PyList_Size(res1.ptr()), 4);

        // second w. resolve
        auto res2 = ctx.parallelize(object_list).map("lambda x: 1. / x", "")
           .resolve(ecToI64(ExceptionCode::ZERODIVISIONERROR), "lambda x: 42", "").collect();
        ASSERT_TRUE(res2.ptr());
        ASSERT_TRUE(PyList_Check(res2.ptr()));
        EXPECT_EQ(PyList_Size(res2.ptr()), 5);

        for(int i = 0; i < PyList_Size(res2.ptr()); ++i)
            ASSERT_TRUE(PyList_GetItem(res2.ptr(), i)); // check it's not null.
    }

}


// check that non-tuplex mapped pyobjects can be collected properly!
TEST_F(WrapperTest, CollectPyObjects) {
    using namespace tuplex;
    using namespace std;

    auto ctx_opts = "{\"webui.enable\": false,"
                    " \"driverMemory\": \"8MB\","
                    " \"partitionSize\": \"256KB\","
                    "\"tuplex.scratchDir\": \"file://" + scratchDir + "\","
                    "\"resolveWithInterpreterOnly\": true}";
    PythonContext ctx("", "", ctx_opts);


    auto L = python::runAndGet("import numpy as np; L = [(1,np.zeros(1)), (4,np.zeros(4))]", "L");
    ASSERT_TRUE(L);
    Py_XINCREF(L);
    auto object_list = py::reinterpret_borrow<py::list>(L);

    {
        // second w. resolve
        auto res = ctx.parallelize(object_list).map("lambda a, b: (a + 1, b)", "").collect();
        ASSERT_TRUE(res.ptr());
        ASSERT_TRUE(PyList_Check(res.ptr()));
        EXPECT_EQ(PyList_Size(res.ptr()), 2);

        // check types and values
        EXPECT_EQ(PyLong_AsLong(PyTuple_GetItem(PyList_GetItem(res.ptr(), 0), 0)), 2);
        auto item = PyTuple_GetItem(PyList_GetItem(res.ptr(), 0), 1);
        EXPECT_EQ(PyObject_Length(PyTuple_GetItem(PyList_GetItem(res.ptr(), 0), 1)), 1);
        EXPECT_EQ(PyLong_AsLong(PyTuple_GetItem(PyList_GetItem(res.ptr(), 1), 0)), 5);
        EXPECT_EQ(PyObject_Length(PyTuple_GetItem(PyList_GetItem(res.ptr(), 1), 1)), 4);


        // trigger GC
        python::runAndGet("import gc; gc.collect()");
    }
}

TEST_F(WrapperTest, SingleCharCSVField) {
    // reported as issue-275, crash when single char field is encountered
    // https://github.com/LeonhardFS/Tuplex/issues/275
    using namespace tuplex;
    using namespace std;

    auto ctx_opts = "{\"webui.enable\": false,"
                    " \"driverMemory\": \"8MB\","
                    " \"partitionSize\": \"256KB\","
                    "\"tuplex.scratchDir\": \"file://" + scratchDir + "\","
                    "\"resolveWithInterpreterOnly\": true}";
    PythonContext ctx("", "", ctx_opts);


    auto L = python::runAndGet("L=['a', 'b', '-']", "L");
    ASSERT_TRUE(L);
    Py_XINCREF(L);
    {
        auto object_list = py::reinterpret_borrow<py::list>(L);
        // save to csv file
        ctx.parallelize(object_list).tocsv("testdata.csv"); // <-- this is not where the failure happens, happens somewhere else?

        auto typehints = PyDict_New();
        PyDict_SetItem(typehints, PyLong_FromLong(0), python::PyString_FromString("str"));

        // read from file incl. type hints
        auto ds = ctx.csv("testdata.part0.csv",py::none(), true, false, "", "\"",
                          py::none(),
                          py::reinterpret_steal<py::dict>(typehints));
    }
}

TEST_F(WrapperTest, NYC311) {
    using namespace tuplex;
    using namespace std;

    auto ctx_opts = "{\"webui.enable\": false,"
                    " \"driverMemory\": \"8MB\","
                    " \"partitionSize\": \"256KB\","
                    "\"executorCount\": 0,"
                    "\"tuplex.scratchDir\": \"file://" + scratchDir + "\","
                    "\"resolveWithInterpreterOnly\": true}";

    auto fix_zip_codes_c = "def fix_zip_codes(zips):\n"
                           "    if not zips:\n"
                           "         return None\n"
                           "    # Truncate everything to length 5 \n"
                           "    s = zips[:5]\n"
                           "    \n"
                           "    # Set 00000 zip codes to nan\n"
                           "    if s == '00000':\n"
                           "         return None\n"
                           "    else:\n"
                           "         return s";

    auto service_path = "../resources/pipelines/311/311_nyc_sample.csv";

    PythonContext ctx("", "", ctx_opts);
    {
        auto type_dict = PyDict_New();
        PyDict_SetItem(type_dict, python::PyString_FromString("Incident Zip"), python::PyString_FromString("str"));
        auto cols_to_select = PyList_New(1);
        PyList_SET_ITEM(cols_to_select, 0, python::PyString_FromString("Incident Zip"));
        // type hints:
        // vector<string>{"Unspecified", "NO CLUE", "NA", "N/A", "0", ""}
        ctx.csv(service_path,py::none(), true, false, "", "\"",
                py::none(), py::reinterpret_steal<py::dict>(type_dict))
                .mapColumn("Incident Zip", fix_zip_codes_c, "")
                .selectColumns(py::reinterpret_borrow<py::list>(cols_to_select))
                .unique().show();

        std::cout<<std::endl;
    }
}

TEST_F(WrapperTest, MixedTypesIsWithNone) {
    using namespace tuplex;
    using namespace std;

    auto opts = microTestOptions();
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    PythonContext c("python", "",  opts.asJSON());

    PyObject *listObj = PyList_New(8);
    PyList_SetItem(listObj, 0, Py_None);
    PyList_SetItem(listObj, 1, PyLong_FromLong(255));
    PyList_SetItem(listObj, 2, PyLong_FromLong(400));
    PyList_SetItem(listObj, 3, Py_True);
    PyList_SetItem(listObj, 4, PyFloat_FromDouble(2.7));
    PyList_SetItem(listObj, 5, PyTuple_New(0)); // empty tuple
    PyList_SetItem(listObj, 6, PyList_New(0)); // empty list
    PyList_SetItem(listObj, 7, PyDict_New()); // empty dict

    auto ref = vector<bool>{true, false, false, false, false, false, false, false};

    Py_IncRef(listObj);

    {
        auto list = py::reinterpret_borrow<py::list>(listObj);
        auto res = c.parallelize(list).map("lambda x: (x, x is None)", "").collect();
        auto resObj = res.ptr();
        PyObject_Print(resObj, stdout, 0);
        std::cout<<std::endl;

        // convert to list and check
        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_Size(resObj), ref.size());

        for(int i = 0; i < ref.size(); ++i) {
            auto item = PyList_GetItem(resObj, i);
            Py_INCREF(item);
            ASSERT_TRUE(PyTuple_Check(item));

            auto el0 = PyTuple_GetItem(item, 0);
            auto el1 = PyTuple_GetItem(item, 1);
            Py_INCREF(el0);
            Py_IncRef(el1);
            ASSERT_TRUE(el1 == Py_False || el1 == Py_True);
            auto el1_true = el1 == Py_True;
            EXPECT_EQ(el1_true, ref[i]);

            auto cmp_res = PyObject_RichCompare(PyList_GetItem(listObj, i), el0, Py_EQ);

            cout<<"comparing "<<python::PyString_AsString(PyList_GetItem(listObj, i))<<" == "<<python::PyString_AsString(el0)<<endl;
            PyObject_Print(cmp_res, stdout, 0);
            std::cout<<std::endl;
            ASSERT_TRUE(cmp_res);
            EXPECT_EQ(cmp_res, Py_True);
        }
    }
}

TEST_F(WrapperTest, SelectColumns) {
    using namespace tuplex;

    auto content = "Unique Key,Created Date,Closed Date,Agency,Agency Name,Complaint Type,Descriptor,Location Type,Incident Zip,Incident Address,Street Name,Cross Street 1,Cross Street 2,Intersection Street 1,Intersection Street 2,Address Type,City,Landmark,Facility Type,Status,Due Date,Resolution Description,Resolution Action Updated Date,Community Board,BBL,Borough,X Coordinate (State Plane),Y Coordinate (State Plane),Open Data Channel Type,Park Facility Name,Park Borough,Vehicle Type,Taxi Company Borough,Taxi Pick Up Location,Bridge Highway Name,Bridge Highway Direction,Road Ramp,Bridge Highway Segment,Latitude,Longitude,Location\n"
                   "19937896,03/01/2011 02:27:57 PM,03/14/2011 03:59:20 PM,DOF,Refunds and Adjustments,DOF Property - Payment Issue,Misapplied Payment,Property Address,10027,,,,,,,ADDRESS,NEW YORK,,N/A,Closed,03/22/2011 02:27:57 PM,The Department of Finance resolved this issue.,03/14/2011 03:59:20 PM,09 MANHATTAN,1019820050,MANHATTAN,,,PHONE,Unspecified,MANHATTAN,,,,,,,,,,\n"
                   "19937901,03/01/2011 10:41:13 AM,03/15/2011 04:14:19 PM,DOT,Department of Transportation,Street Sign - Dangling,Street Cleaning - ASP,Street,11232,186 25 STREET,25 STREET,3 AVENUE,4 AVENUE,,,ADDRESS,BROOKLYN,,N/A,Closed,03/15/2011 05:32:23 PM,The Department of Transportation has completed the request or corrected the condition.,03/15/2011 04:14:19 PM,07 BROOKLYN,3006540024,BROOKLYN,984640,180028,PHONE,Unspecified,BROOKLYN,,,,,,,,40.660811976282695,-73.99859430999363,\"(40.660811976282695, -73.99859430999363)\"\n"
                   "19937902,03/01/2011 09:07:45 AM,03/15/2011 08:26:09 AM,DOT,Department of Transportation,Street Sign - Missing,Other/Unknown,Street,11358,,,,,158 STREET,NORTHERN BOULEVARD,INTERSECTION,FLUSHING,,N/A,Closed,03/15/2011 02:24:33 PM,The Department of Transportation has completed the request or corrected the condition.,03/15/2011 08:26:09 AM,07 QUEENS,,QUEENS,1037621,217498,PHONE,Unspecified,QUEENS,,,,,,,,40.763497105049986,-73.80733639290203,\"(40.763497105049986, -73.80733639290203)\"\n"
                   "19937903,03/01/2011 05:39:26 PM,04/04/2011 11:32:57 AM,DOT,Department of Transportation,Street Sign - Missing,School Crossing,Street,10014,10 SHERIDAN SQUARE,SHERIDAN SQUARE,BARROW STREET,GROVE STREET,,,ADDRESS,NEW YORK,,N/A,Closed,04/01/2011 03:43:12 PM,\"Upon inspection, the reported condition was not found, therefore no action was taken.\",04/04/2011 11:32:57 AM,02 MANHATTAN,1005920040,MANHATTAN,983719,206336,PHONE,Unspecified,MANHATTAN,,,,,,,,40.733021305197404,-74.00191597502526,\"(40.733021305197404, -74.00191597502526)\"\n"
                   "19937904,03/01/2011 11:08:14 AM,03/02/2011 07:55:37 AM,DOT,Department of Transportation,Street Sign - Missing,Stop,Street,10069,,,,,WEST   63 STREET,WEST END AVENUE,INTERSECTION,NEW YORK,,N/A,Closed,03/08/2011 11:08:14 AM,\"The condition has been inspected/investigated, see customer notes for more information.\",03/02/2011 07:55:37 AM,07 MANHATTAN,,MANHATTAN,987400,221308,PHONE,Unspecified,MANHATTAN,,,,,,,,40.77411510013836,-73.98862703263869,\"(40.77411510013836, -73.98862703263869)\"\n"
                   "19937906,03/01/2011 03:16:09 PM,03/02/2011 09:06:30 AM,DOF,Correspondence Unit,DOF Property - Request Copy,Copy of Notice of Property Value,Property Address,11105,,,,,,,ADDRESS,ASTORIA,,N/A,Closed,03/06/2011 03:16:09 PM,The Department of Finance mailed the requested item.,03/02/2011 09:06:31 AM,01 QUEENS,4009650074,QUEENS,,,PHONE,Unspecified,QUEENS,,,,,,,,,,\n"
                   "19937907,03/01/2011 01:22:59 PM,03/02/2011 09:06:28 AM,DOF,Correspondence Unit,DOF Property - Request Copy,Copy of Notice of Property Value,Property Address,10469,,,,,,,ADDRESS,BRONX,,N/A,Closed,03/06/2011 01:22:59 PM,The Department of Finance mailed the requested item.,03/02/2011 09:06:28 AM,12 BRONX,2046970142,BRONX,,,PHONE,Unspecified,BRONX,,,,,,,,,,\n"
                   "19937908,03/01/2011 12:01:58 PM,03/02/2011 09:05:26 AM,DOF,Correspondence Unit,DOF Property - Request Copy,Copy of Notice of Property Value,Property Address,10305,,,,,,,ADDRESS,STATEN ISLAND,,N/A,Closed,03/06/2011 12:01:58 PM,The Department of Finance mailed the requested item.,03/02/2011 09:05:26 AM,02 STATEN ISLAND,5032350004,STATEN ISLAND,,,PHONE,Unspecified,STATEN ISLAND,,,,,,,,,,\n"
                   "19937909,03/01/2011 02:35:46 PM,03/02/2011 09:06:31 AM,DOF,Correspondence Unit,DOF Property - Request Copy,Copy of Notice of Property Value,Property Address,11221,,,,,,,ADDRESS,BROOKLYN,,N/A,Closed,03/06/2011 02:35:46 PM,The Department of Finance mailed the requested item.,03/02/2011 09:06:31 AM,04 BROOKLYN,3033660059,BROOKLYN,,,PHONE,Unspecified,BROOKLYN,,,,,,,,,,";

    // write to test dir!
    auto testURI = testName + "/311_subset.csv";

    stringToFile(testURI, content);

    auto ctx_opts = "{\"webui.enable\": false,"
                    " \"driverMemory\": \"8MB\","
                    " \"partitionSize\": \"256KB\","
                    "\"executorCount\": 0,"
                    "\"tuplex.scratchDir\": \"file://" + scratchDir + "\","
                                                                      "\"resolveWithInterpreterOnly\": true}";

    PythonContext ctx("", "", ctx_opts);
    {
        auto cols_to_select = PyList_New(1);
        PyList_SET_ITEM(cols_to_select, 0, python::PyString_FromString("Unique Key"));

        auto res = ctx.csv(testURI).selectColumns(py::reinterpret_borrow<py::list>(cols_to_select)).columns();
        auto resObj = res.ptr();
        ASSERT_TRUE(PyList_Check(resObj));
        ASSERT_EQ(PyList_Size(resObj), 1);
        auto col_name = python::PyString_AsString(PyList_GET_ITEM(resObj, 0));
        EXPECT_EQ(col_name, "Unique Key");
    }
}

TEST_F(WrapperTest, PartitionRelease) {
    // this test checks that when context is destroyed, partitions are also properly released

      using namespace tuplex;
    using namespace std;

    auto ctx_opts = "{\"webui.enable\": false,"
                    " \"driverMemory\": \"8MB\","
                    " \"partitionSize\": \"256KB\","
                    "\"executorCount\": 0,"
                    "\"tuplex.scratchDir\": \"file://" + scratchDir + "\","
                    "\"resolveWithInterpreterOnly\": true}";

    auto fix_zip_codes_c = "def fix_zip_codes(zips):\n"
                           "    if not zips:\n"
                           "         return None\n"
                           "    # Truncate everything to length 5 \n"
                           "    s = zips[:5]\n"
                           "    \n"
                           "    # Set 00000 zip codes to nan\n"
                           "    if s == '00000':\n"
                           "         return None\n"
                           "    else:\n"
                           "         return s";

    auto service_path = "../resources/pipelines/311/311_nyc_sample.csv";

    PythonContext *ctx = new PythonContext("", "", ctx_opts);

    PythonContext ctx2("", "", ctx_opts);
    {
        auto type_dict = PyDict_New();
        PyDict_SetItem(type_dict, python::PyString_FromString("Incident Zip"), python::PyString_FromString("str"));
        auto cols_to_select = PyList_New(1);
        PyList_SET_ITEM(cols_to_select, 0, python::PyString_FromString("Incident Zip"));
        // type hints:
        // vector<string>{"Unspecified", "NO CLUE", "NA", "N/A", "0", ""}
        ctx->csv(service_path,py::none(), true, false, "", "\"",
                py::none(), py::reinterpret_steal<py::dict>(type_dict))
                .mapColumn("Incident Zip", fix_zip_codes_c, "")
                .selectColumns(py::reinterpret_steal<py::dict>(cols_to_select))
                .unique().show();

        std::cout<<std::endl;

        delete ctx; // should invoke partitions!
        ctx = nullptr;


        type_dict = PyDict_New();
        PyDict_SetItem(type_dict, python::PyString_FromString("Incident Zip"), python::PyString_FromString("str"));
        cols_to_select = PyList_New(1);
        PyList_SET_ITEM(cols_to_select, 0, python::PyString_FromString("Incident Zip"));

        ctx2.csv(service_path,py::none(), true, false, "", "\"",
                 py::none(), py::reinterpret_steal<py::dict>(type_dict))
                .mapColumn("Incident Zip", fix_zip_codes_c, "")
                .selectColumns(py::reinterpret_steal<py::dict>(cols_to_select))
                .unique().show();
    }

}

TEST_F(WrapperTest, TracingVisitorError) {
    using namespace tuplex;

    auto udf_code = "def count(L):\n"
                    "    d = {}\n"
                    "    for x in L:\n"
                    "        if x not in d.keys():\n"
                    "            d[x] = 0\n"
                    "        d[x] += 1\n"
                    "    return d";

    auto ctx_opts = "{\"webui.enable\": false,"
                    " \"driverMemory\": \"8MB\","
                    " \"partitionSize\": \"256KB\","
                    "\"executorCount\": 0,"
                    "\"tuplex.scratchDir\": \"file://" + scratchDir + "\","
                                                                      "\"resolveWithInterpreterOnly\": true}";

    PythonContext ctx("", "", ctx_opts);
    {
        auto cols_to_select = PyList_New(1);
        PyList_SET_ITEM(cols_to_select, 0, python::PyString_FromString("Unique Key"));


        PyObject *listObj = PyList_New(1);
        PyObject *sublistObj1 = PyList_New(2);
        PyList_SET_ITEM(sublistObj1, 0, python::PyString_FromString("a"));
        PyList_SET_ITEM(sublistObj1, 1, python::PyString_FromString("b"));

        PyList_SetItem(listObj, 0, sublistObj1);

        auto list = py::reinterpret_borrow<py::list>(listObj);

        auto res = ctx.parallelize(list).map(udf_code, "").collect();
        auto resObj = res.ptr();
        ASSERT_TRUE(PyList_Check(resObj));
        EXPECT_GE(PyList_Size(resObj), 1);
    }
}

// as reported in https://github.com/tuplex/tuplex/issues/99
TEST_F(WrapperTest, AllRows311) {
    using namespace tuplex;

    std::string test_data = "UniqueKey,CreatedDate,Agency,ComplaintType,Descriptor,IncidentZip,StreetName\n"
                            "46688741,06/30/2020 07:24:41 PM,NYPD,Noise - Residential,Loud Music/Party,10037.0,MADISON AVENUE\n"
                            "53493739,02/28/2022 07:30:31 PM,NYPD,Illegal Parking,Double Parked Blocking Traffic,11203.0,EAST   56 STREET\n"
                            "48262955,11/27/2020 12:00:00 PM,DSNY,Derelict Vehicles,Derelict Vehicles,11203.0,CLARKSON AVENUE\n"
                            "48262956,11/27/2020 12:00:00 PM,DSNY,Derelict Vehicles,Derelict Vehicles,11208.0,SHEPHERD AVENUE\n"
                            "48262957,11/27/2020 12:00:00 PM,DSNY,Derelict Vehicles,Derelict Vehicles,11238.0,BERGEN STREET\n"
                            "46688747,06/30/2020 02:51:45 PM,NYPD,Noise - Vehicle,Engine Idling,10009.0,EAST   12 STREET\n"
                            "46688748,06/30/2020 09:26:45 AM,NYPD,Non-Emergency Police Matter,Face Covering Violation,11204.0,20 AVENUE\n"
                            "48262973,11/27/2020 03:46:00 PM,DEP,Water Quality,unknown odor/taste in drinking water (QA6),10021.0,EAST   70 STREET\n"
                            "53493766,02/28/2022 05:28:38 AM,NYPD,Noise - Vehicle,Car/Truck Horn,11366.0,PARSONS BOULEVARD\n";

    // write test data out to test path
    std::string input_path = this->testName + "_test_311_testfile.csv";
    std::string output_path = this->testName + "_test_311_output.csv";

    stringToFile(input_path, test_data);

    auto udf_code = "def fix_zip_codes(zips):\n"
                    "            if not zips:\n"
                    "                return None\n"
                    "            # Truncate everything to length 5\n"
                    "            s = zips[:5]\n"
                    "\n"
                    "            # Set 00000 zip codes to nan\n"
                    "            if s == \"00000\":\n"
                    "                return None\n"
                    "            else:\n"
                    "                return s";

    auto ctx_opts = "{\"webui.enable\": false,"
                    " \"driverMemory\": \"8MB\","
                    " \"partitionSize\": \"256KB\","
                    "\"executorCount\": 0,"
                    "\"tuplex.scratchDir\": \"file://" + scratchDir + "\","
                                                                      "\"resolveWithInterpreterOnly\": true}";

    //  null_values=["Unspecified", "NO CLUE", "NA", "N/A", "0", ""],
    //            type_hints={0: typing.Optional[str],
    //                        1: typing.Optional[str],
    //                        2: typing.Optional[str],
    //                        3: typing.Optional[str],
    //                        4: typing.Optional[str],
    //                        5: typing.Optional[str],
    //                        }

    PythonContext ctx("", "", ctx_opts);
    {
        auto null_values_obj = PyList_New(6);
        PyList_SetItem(null_values_obj, 0, python::PyString_FromString("Unspecified"));
        PyList_SetItem(null_values_obj, 1, python::PyString_FromString("NO CLUE"));
        PyList_SetItem(null_values_obj, 2, python::PyString_FromString("NA"));
        PyList_SetItem(null_values_obj, 3, python::PyString_FromString("N/A"));
        PyList_SetItem(null_values_obj, 4, python::PyString_FromString("0"));
        PyList_SetItem(null_values_obj, 5, python::PyString_FromString(""));

        auto type_hints_obj = PyDict_New();
        for(unsigned i = 0; i <= 5; ++i)
            PyDict_SetItem(type_hints_obj, PyLong_FromLong(i), python::runAndGet("import typing; x=typing.Optional[str]", "x"));


        auto null_values = py::reinterpret_borrow<py::list>(null_values_obj);
        auto type_hints = py::reinterpret_borrow<py::dict>(type_hints_obj);

        auto res = ctx.csv(input_path, py::none(), true, false, "", "\"", null_values, type_hints)
                      .mapColumn("Incident Zip", udf_code, "")
                      .unique()
                      .collect();
        auto resObj = res.ptr();
        ASSERT_TRUE(PyList_Check(resObj));
        EXPECT_GE(PyList_Size(resObj), 1);
    }
}

//// debug any python module...
///** Takes a path and adds it to sys.paths by calling PyRun_SimpleString.
// * This does rather laborious C string concatenation so that it will work in
// * a primitive C environment.
// *
// * Returns 0 on success, non-zero on failure.
// */
//int add_path_to_sys_module(const char *path) {
//    int ret = 0;
//    const char *prefix = "import sys\nsys.path.append(\"";
//    const char *suffix = "\")\n";
//    char *command = (char*)malloc(strlen(prefix)
//                                  + strlen(path)
//                                  + strlen(suffix)
//                                  + 1);
//    if (! command) {
//        return -1;
//    }
//    strcpy(command, prefix);
//    strcat(command, path);
//    strcat(command, suffix);
//    ret = PyRun_SimpleString(command);
//#ifdef DEBUG
//    printf("Calling PyRun_SimpleString() with:\n");
//    printf("%s", command);
//    printf("PyRun_SimpleString() returned: %d\n", ret);
//    fflush(stdout);
//#endif
//    free(command);
//    return ret;
//}
//
///** This imports a Python module and calls a specific function in it.
// * It's arguments are similar to main():
// * argc - Number of strings in argv
// * argv - Expected to be 4 strings:
// *      - Name of the executable.
// *      - Path to the directory that the Python module is in.
// *      - Name of the Python module.
// *      - Name of the function in the module.
// *
// * The Python interpreter will be initialised and the path to the Python module
// * will be added to sys.paths then the module will be imported.
// * The function will be called with no arguments and its return value will be
// * ignored.
// *
// * This returns 0 on success, non-zero on failure.
// */
//int import_call_execute(int argc, const char *argv[]) {
//    int return_value = 0;
//    PyObject *pModule   = NULL;
//    PyObject *pFunc     = NULL;
//    PyObject *pResult   = NULL;
//
//    if (argc != 4) {
//        fprintf(stderr,
//                "Wrong arguments!"
//                " Usage: %s package_path module function\n", argv[0]);
//        return_value = -1;
//        goto except;
//    }
//    Py_SetProgramName((wchar_t*)argv[0]);
//    Py_Initialize();
//    if (add_path_to_sys_module(argv[1])) {
//        return_value = -2;
//        goto except;
//    }
//    pModule = PyImport_ImportModule(argv[2]);
//    if (! pModule) {
//        fprintf(stderr,
//                "%s: Failed to load module \"%s\"\n", argv[0], argv[2]);
//        return_value = -3;
//        goto except;
//    }
//    pFunc = PyObject_GetAttrString(pModule, argv[3]);
//    if (! pFunc) {
//        fprintf(stderr,
//                "%s: Can not find function \"%s\"\n", argv[0], argv[3]);
//        return_value = -4;
//        goto except;
//    }
//    if (! PyCallable_Check(pFunc)) {
//        fprintf(stderr,
//                "%s: Function \"%s\" is not callable\n", argv[0], argv[3]);
//        return_value = -5;
//        goto except;
//    }
//    pResult = PyObject_CallObject(pFunc, NULL);
//    if (! pResult) {
//        fprintf(stderr, "%s: Function call failed\n", argv[0]);
//        return_value = -6;
//        goto except;
//    }
//#ifdef DEBUG
//        printf("%s: PyObject_CallObject() succeeded\n", argv[0]);
//#endif
//    assert(! PyErr_Occurred());
//    goto finally;
//    except:
//    assert(PyErr_Occurred());
//    PyErr_Print();
//    finally:
//    Py_XDECREF(pFunc);
//    Py_XDECREF(pModule);
//    Py_XDECREF(pResult);
//    Py_Finalize();
//    return return_value;
//}
