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
#include <cmath>
#include <Context.h>
#include "../../utils/include/Utils.h"
#include "TestUtils.h"
#include "jit/RuntimeInterface.h"

class MathFunctionsTest : public TuplexTest {};

TEST_F(MathFunctionsTest, MathLog) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(testOptions());
    auto v1 = c.parallelize({
        Row(M_E), Row(1.0), Row(pow(M_E, 2.0)), Row(pow(M_E, -1.0))
    }).map(UDF("lambda x: math.log(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 1.0);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), 0.0);
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), 2.0);
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), -1.0);

    auto v2 = c.parallelize({
        Row(1), Row(2), Row(3), Row(4)
    }).map(UDF("lambda x: math.log(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v2.size(), 4);
    EXPECT_DOUBLE_EQ(v2[0].getDouble(0), log(1));
    EXPECT_DOUBLE_EQ(v2[1].getDouble(0), log(2));
    EXPECT_DOUBLE_EQ(v2[2].getDouble(0), log(3));
    EXPECT_DOUBLE_EQ(v2[3].getDouble(0), log(4));

    auto v3 = c.parallelize({
        Row(true), Row(false)
    }).map(UDF("lambda x: math.log(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v3.size(), 2);
    EXPECT_DOUBLE_EQ(v3[0].getDouble(0), log(true));
    EXPECT_DOUBLE_EQ(v3[1].getDouble(0), log(false));

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathExp) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(testOptions());
    auto v1 = c.parallelize({
            Row(1.0), Row(0.0), Row(2.0), Row(-1.0)
    }).map(UDF("lambda x: math.exp(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), M_E);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), 1.0);
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), exp(2.0));
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), 1 / M_E);

    auto v2 = c.parallelize({
        Row(1), Row(0), Row(2), Row(-1)
    }).map(UDF("lambda x: math.exp(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v2.size(), 4);
    EXPECT_DOUBLE_EQ(v2[0].getDouble(0), M_E);
    EXPECT_DOUBLE_EQ(v2[1].getDouble(0), 1.0);
    EXPECT_DOUBLE_EQ(v2[2].getDouble(0), exp(2.0));
    EXPECT_DOUBLE_EQ(v2[3].getDouble(0), 1 / M_E);

    auto v3 = c.parallelize({
        Row(true), Row(false)
    }).map(UDF("lambda x: math.exp(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v3.size(), 2);
    EXPECT_DOUBLE_EQ(v3[0].getDouble(0), exp(true));
    EXPECT_DOUBLE_EQ(v3[1].getDouble(0), exp(false));

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathLog1p) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(testOptions());
    auto v1 = c.parallelize({
            Row(0.0), Row(M_E - 1), Row(M_E * M_E - 1), Row(1 / M_E - 1)
    }).map(UDF("lambda x: math.log1p(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 0.0);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), 1.0);
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), 2.0);
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), -1.0);

    auto v2 = c.parallelize({
        Row(0), Row(2), Row(7), Row(1)
    }).map(UDF("lambda x: math.log1p(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v2.size(), 4);
    EXPECT_DOUBLE_EQ(v2[0].getDouble(0), 0.0);
    EXPECT_DOUBLE_EQ(v2[1].getDouble(0), log1p(2));
    EXPECT_DOUBLE_EQ(v2[2].getDouble(0), log1p(7));
    EXPECT_DOUBLE_EQ(v2[3].getDouble(0), log1p(1));

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathLog2) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(testOptions());
    auto v1 = c.parallelize({
            Row(2.0), Row(1.0), Row(4.0), Row(1 / 2)
    }).map(UDF("lambda x: math.log2(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 1.0);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), 0.0);
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), 2.0);
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), log2(1 / 2));

    auto v2 = c.parallelize({
                                    Row(2), Row(1), Row(4), Row(8)
                            }).map(UDF("lambda x: math.log2(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v2.size(), 4);
    EXPECT_DOUBLE_EQ(v2[0].getDouble(0), 1.0);
    EXPECT_DOUBLE_EQ(v2[1].getDouble(0), 0.0);
    EXPECT_DOUBLE_EQ(v2[2].getDouble(0), 2.0);
    EXPECT_DOUBLE_EQ(v2[3].getDouble(0), 3.0);

    auto v3 = c.parallelize({
                                    Row(true), Row(false)
                            }).map(UDF("lambda x: math.log2(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v3.size(), 2);
    EXPECT_DOUBLE_EQ(v3[0].getDouble(0), log2(true));
    EXPECT_DOUBLE_EQ(v3[1].getDouble(0), log2(false));

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathLog10) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(testOptions());
    auto v1 = c.parallelize({
            Row(10.0), Row(1.0), Row(100.0), Row(1 / 100)
    }).map(UDF("lambda x: math.log10(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 1.0);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), 0.0);
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), 2.0);
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), log10(1 / 100));

    auto v2 = c.parallelize({
        Row(10), Row(1), Row(100)
    }).map(UDF("lambda x: math.log10(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v2.size(), 3);
    EXPECT_DOUBLE_EQ(v2[0].getDouble(0), 1.0);
    EXPECT_DOUBLE_EQ(v2[1].getDouble(0), 0.0);
    EXPECT_DOUBLE_EQ(v2[2].getDouble(0), 2.0);

    auto v3 = c.parallelize({
                                    Row(true), Row(false)
                            }).map(UDF("lambda x: math.log10(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v3.size(), 2);
    EXPECT_DOUBLE_EQ(v3[0].getDouble(0), log10(true));
    EXPECT_DOUBLE_EQ(v3[1].getDouble(0), log10(false));


    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathExpm1) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(testOptions());
    auto v1 = c.parallelize({
            Row(0.0), Row(1.0), Row(2.0), Row(-1.0)
    }).map(UDF("lambda x: math.expm1(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), expm1(0.0));
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), expm1(1.0));
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), expm1(2.0));
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), expm1(-1.0));

    auto v2 = c.parallelize({
        Row(0), Row(1), Row(2), Row(-1)
    }).map(UDF("lambda x: math.expm1(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_DOUBLE_EQ(v2[0].getDouble(0), expm1(0.0));
    EXPECT_DOUBLE_EQ(v2[1].getDouble(0), expm1(1.0));
    EXPECT_DOUBLE_EQ(v2[2].getDouble(0), expm1(2.0));
    EXPECT_DOUBLE_EQ(v2[3].getDouble(0), expm1(-1.0));

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathSin) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());
    auto v1 = c.parallelize({
        Row(0.0), Row(M_PI), Row(2 * M_PI), Row(-1.0 * M_PI)
    }).map(UDF("lambda x: math.sin(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 0.0);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), sin(M_PI));
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), sin(2 * M_PI));
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), sin(-1.0 * M_PI));

    auto v2 = c.parallelize({
        Row(1 / 2 * M_PI), Row(3 / 2 * M_PI), Row(-1 / 2 * M_PI)
    }).map(UDF("lambda x: math.sin(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v2.size(), 3);
    EXPECT_DOUBLE_EQ(v2[0].getDouble(0), sin(1 / 2 * M_PI));
    EXPECT_DOUBLE_EQ(v2[1].getDouble(0), sin(3 / 2 * M_PI));
    EXPECT_DOUBLE_EQ(v2[2].getDouble(0), sin(-1 / 2 * M_PI));

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathSinH) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());
    auto v1 = c.parallelize({
                                    Row(0.0)
                            }).map(UDF("lambda x: math.sinh(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 1);
    EXPECT_EQ(v1[0].getDouble(0), 0);

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathArcSinH) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());
    auto v1 = c.parallelize({
                                    Row(0.0), Row(1.0), Row(-1.0)
                            }).map(UDF("lambda x: math.asinh(x)", "", ce)).collectAsVector();
    EXPECT_EQ(v1.size(), 3);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 0);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), 0.88137358701954305);
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), -0.88137358701954305);

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathCos) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());
    auto v1 = c.parallelize({
        Row(0.0), Row(M_PI), Row(2 * M_PI), Row(-1.0 * M_PI)
    }).map(UDF("lambda x: math.cos(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), cos(0));
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), cos(M_PI));
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), cos(2 * M_PI));
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), cos(-1.0 * M_PI));

    auto v2 = c.parallelize({
        Row(1 / 2 * M_PI), Row(3 / 2 * M_PI), Row(-1 / 2 * M_PI)
    }).map(UDF("lambda x: math.cos(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v2.size(), 3);
    EXPECT_DOUBLE_EQ(v2[0].getDouble(0), cos(1 / 2 * M_PI));
    EXPECT_DOUBLE_EQ(v2[1].getDouble(0), cos(3 / 2 * M_PI));
    EXPECT_DOUBLE_EQ(v2[2].getDouble(0), cos(-1 / 2 * M_PI));

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathCosH) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());
    auto v1 = c.parallelize({
                                    Row(0.0)
                            }).map(UDF("lambda x: math.cosh(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 1);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 1);

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathArcCos) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());
    auto v1 = c.parallelize({
                                    Row(-1.0), Row(0.0), Row(1.0)
                            }).map(UDF("lambda x: math.acos(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 3);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), M_PI);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), M_PI/2);
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), 0);

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathTanH) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());
    double inf = std::numeric_limits<double>::infinity();
    auto v1 = c.parallelize({
                                    Row(0.0), Row(inf), Row(-inf)
                            }).map(UDF("lambda x: math.tanh(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 3);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 0);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), 1);
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), -1);

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathArcTan) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());
    double inf = std::numeric_limits<double>::infinity();
    auto v1 = c.parallelize({
                                    Row(-1.0), Row(0.0), Row(1.0), Row(inf), Row(-inf)
                            }).map(UDF("lambda x: math.atan(x)", "", ce)).collectAsVector();
    EXPECT_EQ(v1.size(), 5);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), -M_PI/4);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), 0);
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), M_PI/4);
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), M_PI/2);
    EXPECT_DOUBLE_EQ(v1[4].getDouble(0), -M_PI/2);

    python::lockGIL();
    python::closeInterpreter();
}


TEST_F(MathFunctionsTest, MathArcTanH) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());
    auto v1 = c.parallelize({
                                    Row(0.0), Row(0.5), Row(-0.5)
                            }).map(UDF("lambda x: math.atanh(x)", "", ce)).collectAsVector();
    EXPECT_EQ(v1.size(), 3);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 0);
    EXPECT_DOUBLE_EQ(abs(v1[1].getDouble(0)), 0.54930614433405489);
    EXPECT_DOUBLE_EQ(abs(v1[2].getDouble(0)), 0.54930614433405489);
    python::lockGIL();
    python::closeInterpreter();
}


TEST_F(MathFunctionsTest, MathRadians) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());
    auto v1 = c.parallelize({
                                    Row(180.0), Row(90.0), Row(-45.0), Row(0.0)
                            }).map(UDF("lambda x: math.radians(x)", "", ce)).collectAsVector();
    EXPECT_EQ(v1.size(), 4);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), M_PI);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), M_PI/2);
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), -M_PI/4);
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), 0);

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathDegrees) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());
    auto v1 = c.parallelize({
                                    Row(M_PI), Row(M_PI/2), Row(-M_PI/4), Row(0.0)
                            }).map(UDF("lambda x: math.degrees(x)", "", ce)).collectAsVector();
    EXPECT_EQ(v1.size(), 4);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 180);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), 90);
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), -45);
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), 0);

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathArcTan2) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");
    Context c(microTestOptions());
    auto v1 = c.parallelize({
                                    Row(-1, 0), Row(-1, 1), Row(0, 1), Row(1, 1), Row(1, 0)
                            }).map(UDF("lambda x, y: math.atan2(x, y)", "", ce)).collectAsVector();
    EXPECT_EQ(v1.size(), 5);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), -M_PI/2);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), -M_PI/4);
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), 0);
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), M_PI/4);
    EXPECT_DOUBLE_EQ(v1[4].getDouble(0), M_PI/2);

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathArcCosH) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());
    auto v1 = c.parallelize({
                                    Row(1.0), Row(2.0)
                            }).map(UDF("lambda x: math.acosh(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 2);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 0);
    ASSERT_TRUE( abs(v1[1].getDouble(0) - 1.31696) <= 0.00001);

    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathSqrt) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(testOptions());
    auto v1 = c.parallelize({
        Row(100.0), Row(1.0), Row(0.0), Row(1 / 100)
    }).map(UDF("lambda x: math.sqrt(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 10.0);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), 1.0);
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), 0.0);
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), sqrt(1 / 100));


    python::lockGIL();
    python::closeInterpreter();
}

TEST_F(MathFunctionsTest, MathAsin) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());
    auto v1 = c.parallelize({
        Row(0.0), Row(sin(M_PI)), Row(sin(2 * M_PI)), Row(sin(-1.0 * M_PI))
    }).map(UDF("lambda x: math.asin(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 0.0);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), asin(sin(M_PI)));
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), asin(sin(2 * M_PI)));
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), asin(sin(-1.0 * M_PI)));

    auto v2 = c.parallelize({
        Row(sin(0.5 * M_PI)), Row(sin(3.0 / 2.0 * M_PI)), Row(sin(-0.5 * M_PI))
    }).map(UDF("lambda x: math.asin(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v2.size(), 3);
    EXPECT_DOUBLE_EQ(v2[0].getDouble(0), 0.5 * M_PI);
    EXPECT_DOUBLE_EQ(v2[1].getDouble(0), asin(sin(3.0 / 2.0 * M_PI)));
    EXPECT_DOUBLE_EQ(v2[2].getDouble(0), -0.5 * M_PI);

    python::lockGIL();
    python::closeInterpreter();
}


TEST_F(MathFunctionsTest, MathPow) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(microTestOptions());
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    auto v1 = c.parallelize({
        Row(3.0, 2.0), Row(1.0, 0.0), Row(1.0, 1.0), Row(2.0, 1.0), Row(3.0, 0.0)
    }).map(UDF("lambda x, y: math.pow(x, y)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 5);
    EXPECT_DOUBLE_EQ(v1[0].getDouble(0), 9.0);
    EXPECT_DOUBLE_EQ(v1[1].getDouble(0), pow(1.0, 0));
    EXPECT_DOUBLE_EQ(v1[2].getDouble(0), pow(1.0, 1));
    EXPECT_DOUBLE_EQ(v1[3].getDouble(0), pow(2, 1.0));
    EXPECT_DOUBLE_EQ(v1[4].getDouble(0), pow(3.0, 0));

    auto v2 = c.parallelize({
        Row(2.0), Row(1.0), Row(-1.0), Row(-2.0), Row(0.0)
    }).map(UDF("lambda y: math.pow(y, 5.0)", "", ce)).collectAsVector();

    EXPECT_EQ(v2.size(), 5);
    EXPECT_DOUBLE_EQ(v2[0].getDouble(0), 32.0);
    EXPECT_DOUBLE_EQ(v2[1].getDouble(0), 1.0);
    EXPECT_DOUBLE_EQ(v2[2].getDouble(0), -1.0);
    EXPECT_DOUBLE_EQ(v2[3].getDouble(0), -32.0);
    EXPECT_DOUBLE_EQ(v2[4].getDouble(0), 0.0);

    auto v3 = c.parallelize({
        Row(-2.0), Row(0.0), Row(-1.0), Row(5.0), Row(1.0)
    }).map(UDF("lambda x: math.pow(-2.0, x)", "", ce)).collectAsVector();

    EXPECT_EQ(v3.size(), 5);
    EXPECT_DOUBLE_EQ(v3[0].getDouble(0), pow(-2, -2));
    EXPECT_DOUBLE_EQ(v3[1].getDouble(0), pow(-2.0, 0.0));
    EXPECT_DOUBLE_EQ(v3[2].getDouble(0), pow(-2.0, -1.0));
    EXPECT_DOUBLE_EQ(v3[3].getDouble(0), -32.0);
    ASSERT_DOUBLE_EQ(v3[4].getDouble(0), -2.0);

    auto v4 = c.parallelize({
        Row(2), Row(1), Row(-1), Row(-2), Row(0)
    }).map(UDF("lambda y: math.pow(y, 5)", "", ce)).collectAsVector();

    EXPECT_EQ(v4.size(), 5);
    EXPECT_DOUBLE_EQ(v4[0].getDouble(0), 32.0);
    EXPECT_DOUBLE_EQ(v4[1].getDouble(0), 1.0);
    EXPECT_DOUBLE_EQ(v4[2].getDouble(0), -1.0);
    EXPECT_DOUBLE_EQ(v4[3].getDouble(0), -32.0);
    EXPECT_DOUBLE_EQ(v4[4].getDouble(0), 0.0);

    auto v5 = c.parallelize({
        Row(true, true), Row(false, false), Row(true, false), Row(false, true)
    }).map(UDF("lambda x, y: math.pow(x, y)", "", ce)).collectAsVector();

    EXPECT_EQ(v5.size(), 4);
    EXPECT_DOUBLE_EQ(v5[0].getDouble(0), pow(true, true));
    EXPECT_DOUBLE_EQ(v5[1].getDouble(0), pow(false, false));
    EXPECT_DOUBLE_EQ(v5[2].getDouble(0), pow(true, false));
    EXPECT_DOUBLE_EQ(v5[3].getDouble(0), pow(false, true));

    python::lockGIL();
    python::closeInterpreter();
}


TEST_F(MathFunctionsTest, MathIsNan) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(microTestOptions());
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    auto v1 = c.parallelize({
                                    Row(0.0), Row(DOUBLE_QUIET_NAN), Row(INFINITY), Row(-INFINITY)
    }).map(UDF("lambda x: math.isnan(x)", "", ce)).collectAsVector();

    EXPECT_EQ(v1.size(), 4);
    EXPECT_EQ(v1[0].getBoolean(0), false);
    EXPECT_EQ(v1[1].getBoolean(0), true);
    EXPECT_EQ(v1[2].getBoolean(0), false);
    EXPECT_EQ(v1[3].getBoolean(0), false);

    auto v2 = c.parallelize({
        Row(0), Row(-1), Row(5), Row(-97)
    }).map(UDF("lambda x: math.isnan(x)", "", ce)).collectAsVector();
    EXPECT_EQ(v2.size(), 4);
    EXPECT_EQ(v2[0].getBoolean(0), false);
    EXPECT_EQ(v2[1].getBoolean(0), false);
    EXPECT_EQ(v2[2].getBoolean(0), false);
    EXPECT_EQ(v2[3].getBoolean(0), false);

    auto v3 = c.parallelize({
        Row(true), Row(false), Row(true)
    }).map(UDF("lambda x: math.isnan(x)", "", ce)).collectAsVector();
    EXPECT_EQ(v3.size(), 3);
    EXPECT_EQ(v3[0].getBoolean(0), false);
    EXPECT_EQ(v3[1].getBoolean(0), false);
    EXPECT_EQ(v3[2].getBoolean(0), false);

    auto v4 = c.parallelize({
        Row(-0.89), Row(10.23), Row(-97.484), Row(-DOUBLE_QUIET_NAN)
    }).map(UDF("lambda x: math.isnan(x)", "", ce)).collectAsVector();
    EXPECT_EQ(v4.size(), 4);
    EXPECT_EQ(v4[0].getBoolean(0), false);
    EXPECT_EQ(v4[1].getBoolean(0), false);
    EXPECT_EQ(v4[2].getBoolean(0), false);
    EXPECT_EQ(v4[3].getBoolean(0), true);

    python::lockGIL();
    python::closeInterpreter();
}


TEST_F(MathFunctionsTest, MathIsInf) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(microTestOptions());
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    auto v1 = c.parallelize({
                                    Row(M_PI), Row(DOUBLE_QUIET_NAN), Row(INFINITY), Row(-INFINITY)
    }).map(UDF("lambda x: math.isinf(x)", "", ce)).collectAsVector();
    EXPECT_EQ(v1.size(), 4);
    EXPECT_EQ(v1[0].getBoolean(0), false);
    EXPECT_EQ(v1[1].getBoolean(0), false);
    EXPECT_EQ(v1[2].getBoolean(0), true);
    EXPECT_EQ(v1[3].getBoolean(0), true);

    auto v2 = c.parallelize({
        Row(0), Row(-1), Row(5), Row(-97)
    }).map(UDF("lambda x: math.isinf(x)", "", ce)).collectAsVector();
    EXPECT_EQ(v2.size(), 4);
    EXPECT_EQ(v2[0].getBoolean(0), false);
    EXPECT_EQ(v2[1].getBoolean(0), false);
    EXPECT_EQ(v2[2].getBoolean(0), false);
    EXPECT_EQ(v2[3].getBoolean(0), false);

    auto v3 = c.parallelize({
        Row(1.5), Row(-0.89), Row(10.23), Row(-97.484), Row(-INFINITY)
    }).map(UDF("lambda x: math.isinf(x)", "", ce)).collectAsVector();
    EXPECT_EQ(v3.size(), 5);
    EXPECT_EQ(v3[0].getBoolean(0), false);
    EXPECT_EQ(v3[1].getBoolean(0), false);
    EXPECT_EQ(v3[2].getBoolean(0), false);
    EXPECT_EQ(v3[3].getBoolean(0), false);
    EXPECT_EQ(v3[4].getBoolean(0), true);

    auto v4 = c.parallelize({
        Row(true), Row(false)
    }).map(UDF("lambda x: math.isinf(x)", "", ce)).collectAsVector();
    EXPECT_EQ(v4.size(), 2);
    EXPECT_EQ(v4[0].getBoolean(0), false);
    EXPECT_EQ(v4[1].getBoolean(0), false);

    python::lockGIL();
    python::closeInterpreter();
}


TEST_F(MathFunctionsTest, MathIsClose) {
    using namespace std;
    using namespace tuplex;

    python::initInterpreter();
    python::unlockGIL();

    Context c(microTestOptions());
    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    auto v1 = c.parallelize({
        Row(-0.5, 0.0), Row(0.5, 0.50001), Row(0.5, 0.500000005), Row(-0.5, -0.5000000001), Row(0.5, 0.50000000005)
    }).map(UDF("lambda x, y: math.isclose(x, y)", "", ce)).collectAsVector();
    
    EXPECT_EQ(v1.size(), 5);
    EXPECT_EQ(v1[0].getBoolean(0), false);
    EXPECT_EQ(v1[1].getBoolean(0), false);
    EXPECT_EQ(v1[2].getBoolean(0), false);
    EXPECT_EQ(v1[3].getBoolean(0), true);
    EXPECT_EQ(v1[4].getBoolean(0), true);

    auto v2 = c.parallelize({
        Row(0.5, 0.0, 1e-09, 1e-09), Row(0.5, 0.500000005, 5e-09, 0.5)
    }).map(UDF("lambda x, y, r, a: math.isclose(x, y, r, a)", "", ce)).collectAsVector();

    EXPECT_EQ(v2.size(), 2);
    EXPECT_EQ(v2[0].getBoolean(0), false);
    EXPECT_EQ(v2[1].getBoolean(0), true);

    auto v2_1 = c.parallelize({
        Row(0.5, 0.50001, 5e-09)
    }).map(UDF("lambda x, y, r: math.isclose(x, y, r)", "", ce)).collectAsVector();

    EXPECT_EQ(v2_1.size(), 1);
    EXPECT_EQ(v2_1[0].getBoolean(0), false);

    auto v3 = c.parallelize({
        Row(0, 0), Row(0, -1), Row(5, 128)
    }).map(UDF("lambda x, y: math.isclose(x, y)", "", ce)).collectAsVector();

    EXPECT_EQ(v3.size(), 3);
    EXPECT_EQ(v3[0].getBoolean(0), true);
    EXPECT_EQ(v3[1].getBoolean(0), false);
    EXPECT_EQ(v3[2].getBoolean(0), false);

    auto v4 = c.parallelize({
        Row(0, 0, 1e-09, 1e-09), Row(5, 10, 0.5, 15)
    }).map(UDF("lambda x, y, r, a: math.isclose(x, y, r, a)", "", ce)).collectAsVector();

    EXPECT_EQ(v4.size(), 2);
    EXPECT_EQ(v4[0].getBoolean(0), true);
    EXPECT_EQ(v4[1].getBoolean(0), true);

    auto v5 = c.parallelize({
        Row(0, 1, 5e-09)
    }).map(UDF("lambda x, y, r: math.isclose(x, y, r)", "", ce)).collectAsVector();

    EXPECT_EQ(v5.size(), 1);
    EXPECT_EQ(v5[0].getBoolean(0), false);

    auto v6 = c.parallelize({
        Row(true, true, false, false), Row(true, false, true, 1)
    }).map(UDF("lambda x, y, r, a: math.isclose(x, y, r, a)", "", ce)).collectAsVector();

    EXPECT_EQ(v6.size(), 2);
    EXPECT_EQ(v6[0].getBoolean(0), true);
    EXPECT_EQ(v6[1].getBoolean(0), true);

    auto v7 = c.parallelize({
        Row(false, true, 5e-09), Row(false, true, 1.0), Row(false, true, 0.999999)
    }).map(UDF("lambda x, y, r: math.isclose(x, y, r)", "", ce)).collectAsVector();

    EXPECT_EQ(v7.size(), 3);
    EXPECT_EQ(v7[0].getBoolean(0), false);
    EXPECT_EQ(v7[1].getBoolean(0), true);
    EXPECT_EQ(v7[2].getBoolean(0), false);

    auto v8 = c.parallelize({
        Row(0.5, 1, 1e-09, 1e-09), Row(2.0000000009, 2, 5e-09, 0)
    }).map(UDF("lambda x, y, r, a: math.isclose(x, y, r, a)", "", ce)).collectAsVector();

    EXPECT_EQ(v8.size(), 2);
    EXPECT_EQ(v8[0].getBoolean(0), false);
    EXPECT_EQ(v8[1].getBoolean(0), true);

    auto v9 = c.parallelize({
        Row(1, true, 5e-09), Row(1, false, 1e-09)
    }).map(UDF("lambda x, y, r: math.isclose(x, y, r)", "", ce)).collectAsVector();

    EXPECT_EQ(v9.size(), 2);
    EXPECT_EQ(v9[0].getBoolean(0), true);
    EXPECT_EQ(v9[1].getBoolean(0), false);
    
    auto v10 = c.parallelize({
        Row(1.0000000009, true), Row(0.0000000001, false)
    }).map(UDF("lambda x, y: math.isclose(x, y)", "", ce)).collectAsVector();

    EXPECT_EQ(v10.size(), 2);
    EXPECT_EQ(v10[0].getBoolean(0), true);
    EXPECT_EQ(v10[1].getBoolean(0), false);
    
    auto v11 = c.parallelize({
        Row(INFINITY, INFINITY),
        Row(INFINITY, -INFINITY),
        Row(-INFINITY, -INFINITY),
        Row(INFINITY, 5),
        Row(DOUBLE_QUIET_NAN, DOUBLE_QUIET_NAN),
        Row(M_PI, M_PI),
        Row(M_PI, 3.14159265)
    }).map(UDF("lambda x, y: math.isclose(x, y)", "", ce)).collectAsVector();

    EXPECT_EQ(v11.size(), 7);
    EXPECT_EQ(v11[0].getBoolean(0), true);
    EXPECT_EQ(v11[1].getBoolean(0), false);
    EXPECT_EQ(v11[2].getBoolean(0), true);
    EXPECT_EQ(v11[3].getBoolean(0), false);
    EXPECT_EQ(v11[4].getBoolean(0), false);
    EXPECT_EQ(v11[5].getBoolean(0), true);
    EXPECT_EQ(v11[6].getBoolean(0), false);

    python::lockGIL();
    python::closeInterpreter();
}

