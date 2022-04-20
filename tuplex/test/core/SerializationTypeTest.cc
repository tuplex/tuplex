//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Yunzhi Shao first on 4/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include "gtest/gtest.h"
#include <PythonHelpers.h>

using namespace tuplex;

class SerializationTypeTest : public ::testing::Test {
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

namespace python {
    void testRowSerializationType(const std::string& PyLiteral, const std::string& expectedType, bool autoUpcast) {
        auto PyObj = python::runAndGet("obj = " + PyLiteral, "obj");
        auto rowType = python::mapPythonClassToTuplexType(PyObj, autoUpcast);
        auto row = python::pythonToRow(PyObj, rowType, false);
        EXPECT_EQ(row.getRowType().desc(), expectedType);
    }
}

TEST_F(SerializationTypeTest, ListSerializationTest) {
    python::testRowSerializationType("([1, 2, None], ['ab', None])", "([Option[i64]],[Option[str]])", false);
    python::testRowSerializationType("([([[4444, None], [3, 4]], '!!!!!'), ([[-20000, 400], [3, 4]], None)])", "([([[Option[i64]]],Option[str])])", false);
    python::testRowSerializationType("([['aa', 'bbbb', None], None, ['test']])", "([Option[[Option[str]]]])", false);
    python::testRowSerializationType("([([('--', [1000, 2000, None])], [1.25, 5544.2211])])", "([([(str,[Option[i64]])],[f64])])", false);
}

TEST_F(SerializationTypeTest, OptionTupleSerializationTest) {
    python::testRowSerializationType("([(100, -10000000000), None, (5, 2147483647)])", "([Option[(i64,i64)]])", false);
    python::testRowSerializationType("([('string', None, False), (None, (1, [1, 2]), None)])", "([(Option[str],Option[(i64,[i64])],Option[boolean])])", false);
    python::testRowSerializationType("[(1, (1, 2)) ,(2, None)])]", "([(Option[str],Option[(i64,[i64])],Option[boolean])])", false);
    python::testRowSerializationType("('qwert', [ ((False, 2), True, 'ab'), None, (None, False, None), ((None, None), True, 'efghijk')])", "(str,[Option[(Option[(Option[boolean],Option[i64])],boolean,Option[str])]])", false);
}