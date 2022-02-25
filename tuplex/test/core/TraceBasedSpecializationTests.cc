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


// @TODO:
// func traceFlightsPipeline(rows, pip) ....
// func traceRows(...) ... --> convert rows to pyobjects & trace them then!

// then, draw sample from all files, trace all these rows. check accesses

// then, draw sample each file, check accesses.

// can delay parsing be differently used???


// in the trace it would be interesting to know for each input var, how often it get's accessed.
// -> that's important for the delayed parsing optimization.
// if it's only accessed once (or never?) then could use delayed parsing if it's a small string item or so!

TEST_F(SamplingTest, BasicAccessChecks) {
    using namespace std;
    using namespace tuplex;

    // i.e., here x is accessed twice!
    const std::string code_simple = "lambda x: (x + 1) * x";
    const std::string code_def = "def f(x):\n\ty = 2 * x\n\tx = 20\n\treturn y\n";

    const std::string code_multiparam = "lambda a, b, c, d, e, f: a + c * (f + 1)";
    const std::string code_tupledef = "def f(x):\n\treturn x[1] + x[45] + x[2] * x[10] + 2\n";
    // for this func here, x should be accessed once!
    // def f(x):
    //   y = 2 * x
    //   x = 20
    //   return y


    // @TODO: need to account for the cases of a, b, c
    // and the tuple...
    // => dict unrolling?

    auto ast_simple = tuplex::parseToAST(code_simple);
    EXPECT_TRUE(ast_simple);
    auto ast_def = tuplex::parseToAST(code_def);
    EXPECT_TRUE(ast_def);

    auto ast_multiparam = tuplex::parseToAST(code_multiparam);
    EXPECT_TRUE(ast_multiparam);
    auto ast_tupledef = tuplex::parseToAST(code_tupledef);
    EXPECT_TRUE(ast_tupledef);

    python::lockGIL();

    TraceVisitor tv;
    tv.recordTrace(ast_simple, PyLong_FromLong(10));
    EXPECT_EQ(tv.columnAccesses().front(), 2);

    // check one
    TraceVisitor tv_def;
    tv_def.recordTrace(ast_def, PyLong_FromLong(10));
    EXPECT_EQ(tv_def.columnAccesses().front(), 1);
    tv_def.recordTrace(ast_def, PyLong_FromLong(42));
    EXPECT_EQ(tv_def.columnAccesses().front(), 2);

    // now trace tuple based solution...!
    // case 1: a, b, c, ...
    // case 2: x[0], x[34], ...

    auto t1 = PyTuple_New(6);
    for(int i = 0 ; i < 6; ++i)
        PyTuple_SET_ITEM(t1, i, PyLong_FromLong(i));
    TraceVisitor tv_multiparam;
    tv_multiparam.recordTrace(ast_multiparam, t1);
    auto acc1 = tv_multiparam.columnAccesses();

    ASSERT_EQ(acc1.size(), 6);
    EXPECT_EQ(acc1[0], 1);
    EXPECT_EQ(acc1[1], 0);
    EXPECT_EQ(acc1[2], 1);
    EXPECT_EQ(acc1[3], 0);
    EXPECT_EQ(acc1[4], 0);
    EXPECT_EQ(acc1[5], 1);

    // check now tuple syntax
    auto t2 = PyTuple_New(120);
    for(int i = 0 ; i < 120; ++i)
        PyTuple_SET_ITEM(t2, i, PyLong_FromLong(i));
    TraceVisitor tv_tupledef;
    tv_tupledef.recordTrace(ast_tupledef, t2);
    auto acc2 = tv_tupledef.columnAccesses();

    ASSERT_EQ(acc2.size(), 120);
    for(int i = 0 ; i < 120; ++i) {
        if(i == 1 || i == 2 || i == 45 || i == 10) {
            EXPECT_EQ(acc2[i], 1);
        } else {
            EXPECT_EQ(acc2[i], 0);
        }
    }

    // done.
    python::unlockGIL();
}

// Experiment: what we want to do is check for each flights file, how many delayed parsing optimizations we should apply.
// i.e., let's do that via rule of how often the access is ok.
// this helps with serializing etc.!

TEST_F(SamplingTest, FlightsTracing) {
    using namespace std;
    using namespace tuplex;

    string f_path = "../resources/flights_on_time_performance_2019_01.sample.csv";
    auto content = fileToString(f_path);
    // parse into rows
    auto rows = parseRows(content.c_str(), content.c_str() + content.length(), {""});

    // drop the first row because it's the header...
    auto header = rows.front();
    rows = std::vector<Row>(rows.begin() + 1, rows.end());

    cout<<"parsed "<<rows.size()<<" rows"<<endl;
    for(auto row : rows) {
        cout<<row.getRowType().desc()<<endl;
    }
    // trace some stage of the pipeline now!

}