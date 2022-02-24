//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 2/9/2022                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//
#include "gtest/gtest.h"

#include <PythonDataSet.h>
#include <PythonContext.h>
#include <PythonHelpers.h>
#include <PythonWrappers.h>

class RefTest : public ::testing::Test {
protected:
    std::string testName;
    std::string scratchDir;

    void SetUp() override {
        testName = std::string(::testing::UnitTest::GetInstance()->current_test_info()->test_case_name()) + std::string(::testing::UnitTest::GetInstance()->current_test_info()->name());
        scratchDir = "/tmp/" + testName;

        python::initInterpreter();

        // hold GIL
        assert(python::holdsGIL());

        python::runAndGet("import gc; gc.disable()"); // disable python GC so no GC runs are triggered messing with refcounts below
    }

    void TearDown() override {
        python::runAndGet("import gc; gc.collect(); gc.enable()");
        // important to get GIL for this
        python::closeInterpreter();
    }
};

// following tests are meant to illustrate interaction with pybind11 & ref counting


TEST_F(RefTest, BasicTuple) {

    auto empty_tuple = PyTuple_New(0);

    // current ref count
    auto ref_cnt = empty_tuple->ob_refcnt;
    Py_XDECREF(empty_tuple);
    EXPECT_EQ(empty_tuple->ob_refcnt + 1, ref_cnt);

    // tuple set operation steal references, check this here.
    // Note: integers from -5,...255 are cached. So use large integers to create unique, new objects!
    auto el0 = PyLong_FromLong(1024);
    auto el1 = PyLong_FromLong(1025);
    auto el2 = PyLong_FromLong(1026);

    EXPECT_EQ(el0->ob_refcnt, 1);
    EXPECT_EQ(el1->ob_refcnt, 1);
    EXPECT_EQ(el2->ob_refcnt, 1);

    // now save a ref for a single element (el2!)
    Py_XINCREF(el2);
    EXPECT_EQ(el2->ob_refcnt, 2);

    auto tuple = PyTuple_New(3);
    PyTuple_SET_ITEM(tuple, 0, el0);
    PyTuple_SET_ITEM(tuple, 1, el1);
    PyTuple_SET_ITEM(tuple, 2, el2);

    EXPECT_EQ(tuple->ob_refcnt, 1);
    Py_XDECREF(tuple);

    // el0, el1, el2 were stolen - so ref count is 0 AFTER tuple is decref'ed
    // --> in reality though, these objects get trashed/freed immediately. So basically everythng is up in the air
    EXPECT_NE(el0->ob_refcnt, 1);
    EXPECT_NE(el1->ob_refcnt, 1);
    EXPECT_EQ(el2->ob_refcnt, 1); // this element isn't freed, so still good - has ref count 1!

    // trash now too. when dec'ref
    Py_XDECREF(el2);
    EXPECT_NE(el2->ob_refcnt, 1);
}

TEST_F(RefTest, BasicList) {

}