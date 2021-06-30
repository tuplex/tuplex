//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "TestUtils.h"
#include <Context.h>
#include <PythonHelpers.h>

// need for these tests a running python interpreter, so spin it up
class NonCompilableUDFTest : public PyTest {};


TEST(CompileDef, Simple) {
    using namespace tuplex;

    python::initInterpreter();

    std::string code = "def addNumbers(x, y):\n"
                       "  return x + y";

    auto func = python::compileFunction(python::getMainModule(), code);

    // execute
    ExceptionCode ec;
    auto res = python::callFunction(func, python::rowToPython(Row(12, 13)), ec);

    EXPECT_EQ(ec, ExceptionCode::SUCCESS);
    ASSERT_TRUE(PyLong_Check(res));
    EXPECT_EQ(PyLong_AsLong(res), 25);

    Py_XDECREF(func);
    Py_XDECREF(res);

    python::closeInterpreter();
}


TEST_F(NonCompilableUDFTest, SingleDefFunction) {
    using namespace tuplex;

    Context c(microTestOptions());

    std::string code = "def addNumbers(x, y):\n"
                       "  return x + y \n";

    python::lockGIL();

    // pickle
    auto pickled = python::serializeFunction(python::getMainModule(), code);

    // deserialize
    PyObject* func = python::deserializePickledFunction(python::getMainModule(), pickled.c_str(), pickled.length());

    // call function
    ExceptionCode ec;
    auto res = python::callFunctionEx(func, python::rowToPython(Row(12, 14)));

    python::unlockGIL();

    EXPECT_EQ(res.functionName, "addNumbers");
    EXPECT_EQ(res.exceptionCode, ExceptionCode::SUCCESS);
    ASSERT_TRUE(res.res);
    python::lockGIL();
    EXPECT_EQ(PyLong_AsLong(res.res), 26);
    python::unlockGIL();
}


TEST_F(NonCompilableUDFTest, SingleMapNonCompilableFunction) {
    using namespace tuplex;

    Context c(microTestOptions());

    // power is not yet supported
    UDF::disableCompilation();
    UDF udf("lambda x: (x[0], x[0]**x[1])");
    ASSERT_FALSE(udf.isCompiled()); // if this doesn't old the test is pointless

    std::vector<Row> input;
    std::vector<Row> reference;
    size_t numElements = 100;
    for(int i = 0; i < numElements; ++i) {
        input.push_back(Row(i, 2));
        reference.push_back(Row(i, i * i));
    }

    auto result_rows = c.parallelize(input).map(udf).collectAsVector();

    ASSERT_EQ(result_rows.size(), reference.size());
    ASSERT_EQ(result_rows.size(), numElements);
    for(unsigned i = 0; i < numElements; ++i) {
        EXPECT_EQ(result_rows[i], reference[i]);
    }
}

// multithreaded version of test above
TEST_F(NonCompilableUDFTest, SingleMapNonCompilableFunctionMT) {
    using namespace tuplex;

    ContextOptions co = ContextOptions::defaults();

    // lower partition Size to force multitreading & use 8 tasks
    co.set("tuplex.partitionSize", "128B");
    co.set("tuplex.executorCount", "8");
    Context c(co);

    // power is not yet supported
    UDF::disableCompilation();
    UDF udf("lambda x: (x[0], x[0]**x[1])");
    ASSERT_FALSE(udf.isCompiled()); // if this doesn't old the test is pointless


    std::vector<Row> input;
    std::vector<Row> reference;
    size_t numElements = 100;
    for(int i = 0; i < numElements; ++i) {
        input.push_back(Row(i, 2));
        reference.push_back(Row(i, i * i));
    }

    auto result_rows = c.parallelize(input).map(udf).collectAsVector();

    ASSERT_EQ(result_rows.size(), reference.size());
    ASSERT_EQ(result_rows.size(), numElements);
    for(unsigned i = 0; i < numElements; ++i) {
#ifndef NDEBUG
        std::cout<<"result: "<<result_rows[i].toPythonString()<<" reference: "<<reference[i].toPythonString()<<std::endl;
#endif
        EXPECT_EQ(result_rows[i], reference[i]);
    }
}

// multithreaded version with UDFs that are both compulable and not
TEST_F(NonCompilableUDFTest, SingleMapMixedWithFallbackMTOrderI) {
    using namespace tuplex;

    ContextOptions co = ContextOptions::defaults();

    // lower partition Size to force multitreading & use 8 tasks
    co.set("tuplex.partitionSize", "128B");
    co.set("tuplex.executorCount", "8");
    Context c(co);

    // manually disable compilation for this UDF
    UDF::disableCompilation();
    UDF udf("lambda x: (x[0], x[0]**x[1])");
    ASSERT_FALSE(udf.isCompiled()); // if this doesn't old the test is pointless

    UDF::enableCompilation();
    UDF udf2("lambda a, b: (a, b + 1)");
    ASSERT_TRUE(udf2.isCompiled());

    std::vector<Row> input;
    std::vector<Row> reference;
    size_t numElements = 100;
    for(int i = 0; i < numElements; ++i) {
        input.push_back(Row(i, 2));
        reference.push_back(Row(i, i * i + 1));
    }

    auto result_rows = c.parallelize(input).map(udf).map(udf2).collectAsVector();

    ASSERT_EQ(result_rows.size(), reference.size());
    ASSERT_EQ(result_rows.size(), numElements);
    for(unsigned i = 0; i < numElements; ++i) {
#ifndef NDEBUG
        std::cout<<"result: "<<result_rows[i].toPythonString()<<" reference: "<<reference[i].toPythonString()<<std::endl;
#endif
        EXPECT_EQ(result_rows[i], reference[i]);
    }
}

TEST_F(NonCompilableUDFTest, SingleMapMixedWithFallbackMTOrderII) {
    using namespace tuplex;

    ContextOptions co = ContextOptions::defaults();

    // lower partition Size to force multitreading & use 8 tasks
    co.set("tuplex.partitionSize", "128B");
    co.set("tuplex.executorCount", "8");
    Context c(co);

    // manually disable compilation for this UDF
    UDF::disableCompilation();
    UDF udf("lambda x: (x[0] - 1, x[0]**x[1])");
    ASSERT_FALSE(udf.isCompiled()); // if this doesn't old the test is pointless

    UDF::enableCompilation();
    UDF udf2("lambda a, b: (a+1, b)");
    ASSERT_TRUE(udf2.isCompiled());

    std::vector<Row> input;
    std::vector<Row> reference;
    size_t numElements = 100;
    for(int i = 0; i < numElements; ++i) {
        input.push_back(Row(i, 2));
        reference.push_back(Row(i, (i + 1) * (i + 1)));
    }

    auto result_rows = c.parallelize(input).map(udf2).map(udf).collectAsVector();

    ASSERT_EQ(result_rows.size(), reference.size());
    ASSERT_EQ(result_rows.size(), numElements);
    for(unsigned i = 0; i < numElements; ++i) {
#ifndef NDEBUG
        std::cout<<"result: "<<result_rows[i].toPythonString()<<" reference: "<<reference[i].toPythonString()<<std::endl;
#endif
        EXPECT_EQ(result_rows[i], reference[i]);
    }
}

TEST_F(NonCompilableUDFTest, SingleFilterNonCompilableFunction) {
    using namespace tuplex;

    Context c;

    // manually disable compilation for this UDF
    UDF::disableCompilation();
    UDF udf("lambda a: a**2 % 2 == 1");
    ASSERT_FALSE(udf.isCompiled()); // if this doesn't old the test is pointless


    std::vector<Row> input;
    std::vector<Row> reference;
    size_t numElements = 5;
    for(int i = 0; i < numElements; ++i) {
        input.push_back(Row(i));
        if(i * i %2 == 1)
            reference.push_back(Row(i));
    }

    auto result_rows = c.parallelize(input).filter(udf).collectAsVector();

    ASSERT_EQ(result_rows.size(), reference.size());
    for(unsigned i = 0; i < reference.size(); ++i) {
        EXPECT_EQ(result_rows[i], reference[i]);
    }
}

TEST_F(NonCompilableUDFTest, SingleFilterCompilableFunction) {
    using namespace tuplex;

    Context c;

    // manually disable compilation for this UDF
    UDF::disableCompilation();
    UDF udf("lambda a: a > 30");

    std::vector<Row> input;
    std::vector<Row> reference;
    size_t numElements = 100;
    for (int i = 0; i < numElements; ++i) {
        input.push_back(Row(i));
        if (i > 30)
            reference.push_back(Row(i));
    }

    auto result_rows = c.parallelize(input).filter(udf).collectAsVector();

    std::cout << "Hello world" << std::endl;
    ASSERT_EQ(result_rows.size(), reference.size());
    for (unsigned i = 0; i < reference.size(); ++i) {
        std::cout << "result: " << result_rows[i].toPythonString() << " reference: " << reference[i].toPythonString()
                  << std::endl;
        EXPECT_EQ(result_rows[i], reference[i]);
    }
}