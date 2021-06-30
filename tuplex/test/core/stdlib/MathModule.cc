//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Context.h>
#include "../TestUtils.h"

class MathTest : public PyTest {};

TEST_F(MathTest, Constants) {
    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());

    ClosureEnvironment ce;
    ce.importModule("math");

    auto res = c.parallelize({Row(0)}).map(UDF("lambda x: (math.pi, math.e, math.tau, math.inf, math.nan, -math.inf)", "", ce))
    .map(UDF("lambda a,b,c,d,e,f: (str(a), str(b), str(c), str(d), str(e), str(f))")).collectAsVector();

    ASSERT_EQ(res.size(), 1);
    EXPECT_EQ(res[0].toPythonString(), "('3.14159','2.71828','6.28319','inf','nan','-inf')");
}

TEST_F(MathTest, CeilAndFloor) {
    using namespace tuplex;
    using namespace std;

    Context c(microTestOptions());

    // check
    auto& dsB64 = c.parallelize({Row(false), Row(true)});
    vector<Row> refA{Row(0, 0), Row(1, 1)};
    auto& dsI64 = c.parallelize({Row(-20), Row(-2), Row(0), Row(2), Row(30)});
    vector<Row> refB{Row(-20, -20), Row(-2, -2), Row(0, 0), Row(2, 2), Row(30, 30)};
    auto& dsF64 = c.parallelize({Row(-100.0), Row(-4.5), Row(-0.6), Row(-0.2),
                                 Row(0.0), Row(0.2), Row(0.6), Row(7.5), Row(15.0)});
    vector<Row> refC{Row(-100, -100), Row(-4, -5),
                     Row(0, -1), Row(0, -1),
                     Row(0, 0), Row(1, 0),
                     Row(1, 0), Row(8, 7),
                     Row(15, 15)};

    ClosureEnvironment ce;
    ce.fromModuleImport("math", "*");

    // @TODO: check this!
    // for inf, -inf f64 version should throw overflow error. => optimize away in allow undefined behavior
    // for nan should throw value error.
    auto resA = dsB64.map(UDF("lambda x: (ceil(x), floor(x))", "", ce)).collectAsVector();
    ASSERT_EQ(resA.size(), refA.size());
    for(int i = 0; i < resA.size(); ++i)
        EXPECT_EQ(resA[i].toPythonString(), refA[i].toPythonString());

    auto resB = dsI64.map(UDF("lambda x: (ceil(x), floor(x))", "", ce)).collectAsVector();
    ASSERT_EQ(resB.size(), refB.size());
    for(int i = 0; i < resB.size(); ++i)
        EXPECT_EQ(resB[i].toPythonString(), refB[i].toPythonString());

    auto resC = dsF64.map(UDF("lambda x: (ceil(x), floor(x))", "", ce)).collectAsVector();
    ASSERT_EQ(resC.size(), refC.size());
    for(int i = 0; i < resC.size(); ++i)
        EXPECT_EQ(resC[i].toPythonString(), refC[i].toPythonString());

    // Note: could also test exceptions on inf and nan...
}