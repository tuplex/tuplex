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
#include "PythonHelpers.h"

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

TEST_F(SerializationTypeTest, ListSerializationTest) {
    python::testTypeAndSerialization("([1, 2, None], ['ab', None])", "([Option[i64]],[Option[str]])", false);
    python::testTypeAndSerialization("([([[4444, None], [3, 4]], '!!!!!'), ([[-20000, 400], [3, 4]], None)])", "[([[Option[i64]]],Option[str])]", false);
    python::testTypeAndSerialization("([['aa', 'bbbb', None], None, ['test']])", "[Option[[Option[str]]]]", false);
    python::testTypeAndSerialization("([([('--', [1000, 2000, None])], [1.25, 5544.2211])])", "[([(str,[Option[i64]])],[f64])]", false);
}

TEST_F(SerializationTypeTest, OptionTupleSerializationTest) {
    python::testTypeAndSerialization("([(100, -10000000000), None, (5, 2147483647)])", "[Option[(i64,i64)]]", false);
    python::testTypeAndSerialization("([('string', None, False), (None, (1, [1, 2]), None)])", "[(Option[str],Option[(i64,[i64])],Option[boolean])]", false);
}