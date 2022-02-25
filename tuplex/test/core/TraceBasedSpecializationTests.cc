//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 2/25/2022                                                                 //
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

class SamplingTest : public PyTest {};

// in the trace it would be interesting to know for each input var, how often it get's accessed.
// -> that's important for the delayed parsing optimization.
// if it's only accessed once (or never?) then could use delayed parsing if it's a small string item or so!

TEST_F(SamplingTest, BasicAccessChecks) {
    using namespace std;

    const std::string code = "lambda x: x + 1";

    auto ast = tuplex::parseToAST(code);

    python::lockGIL();
    TraceVisitor tv;
    tv.recordTrace(ast, PyLong_FromLong(10));
    python::unlockGIL();
}