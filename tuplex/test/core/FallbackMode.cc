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


class FallbackTest : public PyTest {};

TEST_F(FallbackTest, Numpy_objects) {
    using namespace std;
    using namespace tuplex;

    Context c(microTestOptions());

    ClosureEnvironment ce;
    ce.importModuleAs("np", "numpy"); // misleading name??

    // this is an example of attribute error
    // auto v = c.parallelize({Row(10), Row(20), Row(1)})
    //           .map(UDF("lambda x: np.zeroes(x)", "", ce))
    //           .collectAsVector();

    // run here
    auto v = c.parallelize({Row(10), Row(20), Row(1)})
            .map(UDF("lambda x: np.zeros(x)", "", ce))
            .collectAsVector();

    ASSERT_EQ(v.size(), 3);

    vector<int> ref_sizes{10, 20, 1};
    python::lockGIL();
    // now fetch python objects
    for(int i = 0; i < v.size(); ++i) {
        auto row = v[i];
        ASSERT_EQ(row.getType(0), python::Type::PYOBJECT);
        // get object
        auto f = row.get(0);
        auto obj = python::deserializePickledObject(python::getMainModule(), static_cast<const char *>(f.getPtr()), f.getPtrSize());
        ASSERT_TRUE(obj);
        // compute len
        auto len = PyObject_Length(obj);
        EXPECT_EQ(len, ref_sizes[i]);
    }
    python::unlockGIL();
}

TEST_F(FallbackTest, NonusedExternalMod) {
    using namespace std;
    using namespace tuplex;

    Context c(microTestOptions());

    ClosureEnvironment ce;
    ce.importModuleAs("np", "numpy"); // misleading name??

    auto v = c.parallelize({Row(10), Row(20), Row(1)})
            .map(UDF("lambda x: x + 1", "", ce))
            .collectAsVector();
}

TEST_F(FallbackTest, ArbitraryPyObjectSerialization) {

    using namespace std;
    using namespace tuplex;

    python::lockGIL();
    auto np_obj = python::runAndGet("import numpy as np; X = np.zeros(100)", "X");

    auto np_obj_type = PyObject_Type(np_obj);
    auto np_obj_type_name = python::PyString_AsString(np_obj_type);

    PyObject_Print(np_obj, stdout, 0);
    cout<<endl;

    // convert to tuplex Field (--> PYOBJECT!)
    auto f = python::pythonToField(np_obj);

    EXPECT_EQ(f.getType(), python::Type::PYOBJECT); // mapped to arbitrary python object

    // test fieldToPython...
    auto conv_obj = python::fieldToPython(f);
    ASSERT_TRUE(conv_obj);

    // check it's nd array!
    auto type_obj = PyObject_Type(conv_obj);
    auto name = python::PyString_AsString(type_obj);
    EXPECT_EQ(name, np_obj_type_name);
    PyObject_Print(conv_obj, stdout, 0);
    cout<<endl;
    python::unlockGIL();
}

TEST_F(FallbackTest, NonAccessedPyObjectInPipeline) {

    using namespace std;
    using namespace tuplex;

    python::lockGIL();
    auto np_obj = python::runAndGet("import numpy as np; X = np.zeros(100)", "X");

    auto np_obj_type = PyObject_Type(np_obj);
    auto np_obj_type_name = python::PyString_AsString(np_obj_type);

    PyObject_Print(np_obj, stdout, 0);
    cout << endl;

    // convert to tuplex Field (--> PYOBJECT!)
    auto f = python::pythonToField(np_obj);

    python::unlockGIL();

    // create a simple pipeline, which performs some work but leaves a pyobject untouched...
    Context c(microTestOptions());

    auto v = c.parallelize({Row(10, 20, f), Row(30, 3, f)})
              .map(UDF("lambda a, b, c: (a + b, c)"))
              .collectAsVector();

    ASSERT_EQ(v.size(), 2);
    EXPECT_EQ(v[0].getType(1), python::Type::PYOBJECT);
    EXPECT_EQ(v[1].getType(1), python::Type::PYOBJECT);
    EXPECT_EQ(v[0].getInt(0), 30);
    EXPECT_EQ(v[1].getInt(0), 33);

    std::cout<<"starting tuple flattening test..."<<std::endl; std::cout.flush();

    // because of the tuple flattening, nesting should work as well!
    auto v2 = c.parallelize({Row(10, 20, f), Row(30, 3, f)})
            .map(UDF("lambda a, b, c: (a + b, (c, b))"))
            .collectAsVector();

    ASSERT_EQ(v2.size(), 2);
}

TEST_F(FallbackTest, InvokingNumpyFunctions) {
    // c.parallelize([1, 2, 3, 4]).map(lambda x: [x, x*x, x*x*x]) \
    //                    .map(lambda x: (np.array(x).sum(), np.array(x).mean())).collect()

    using namespace std;
    using namespace tuplex;

    Context c(microTestOptions());

    ClosureEnvironment ce;
    ce.importModuleAs("np", "numpy");

    auto res = c.parallelize({Row(1), Row(2), Row(3), Row(4)})
    .map(UDF("lambda x: [x, x*x, x*x*x]"))
    .map(UDF("lambda x: (np.array(x).sum(), np.array(x).mean())", "", ce))
    .map(UDF("lambda x: (int(x[0]), float(x[1]))")) // cast to regular python objects to trigger compilation.
    .map(UDF("lambda a, b: a if a > b else b"))
    .collectAsVector();

    vector<Row> ref{Row(3), Row(14), Row(39), Row(84)};
    ASSERT_EQ(res.size(), ref.size());

    for(int i = 0; i < ref.size(); ++i) {
        EXPECT_EQ(ref[i].toPythonString(), res[i].toPythonString());
    }
}


TEST_F(FallbackTest, InvokingNumpyFunctionsTwoParamLambda) {

    using namespace std;
    using namespace tuplex;

    Context c(microTestOptions());

    ClosureEnvironment ce;
    ce.importModuleAs("np", "numpy");

    auto res = c.parallelize({Row(1), Row(2), Row(3), Row(4)})
            .map(UDF("lambda x: [x, x*x, x*x*x]"))
            .map(UDF("lambda x: (np.array(x).sum(), np.array(x).mean())", "", ce))
            .map(UDF("lambda a, b: (int(a), float(b))")) // cast to regular python objects to trigger compilation.
            .map(UDF("lambda a, b: a if a > b else b"))
            .collectAsVector();

    vector<Row> ref{Row(3), Row(14), Row(39), Row(84)};
    ASSERT_EQ(res.size(), ref.size());

    for(int i = 0; i < ref.size(); ++i) {
        EXPECT_EQ(ref[i].toPythonString(), res[i].toPythonString());
    }
}