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
#include <Context.h>
#include "TestUtils.h"

class LoopTest : public PyTest {};

TEST_F(LoopTest, TargetTypeAnnotation) {

    using namespace tuplex;

    auto code = "def f(x):\n"
                "  for a in [1, 2, 3, 4]:\n"
                "    x += a\n"
                "  return x";
    UDF udf(code);
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, python::Type::propagateToTupleType(python::Type::I64)));
    auto funcNode = static_cast<NFunction*>(udf.getAnnotatedAST().getFunctionAST());
    auto funcSuite = static_cast<NSuite*>(funcNode->_suite);
    auto forNode = static_cast<NFor*>(funcSuite->_statements[0]);
    EXPECT_EQ(forNode->target->getInferredType(), python::Type::I64);
}

TEST_F(LoopTest, CodegenTestEmptyList) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in []:\n"
                "        x = x + i\n"
                "    return x";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(0));
}

TEST_F(LoopTest, CodegenTestSingleElementList) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in [10]:\n"
                "        x = x * i\n"
                "    return x";

    auto v = c.parallelize({
        Row(10)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(100));
}

TEST_F(LoopTest, CodegenTestListI64) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in [1, 20, 1000, 4]:\n"
                "        x = x * i\n"
                "        x = x + i\n"
                "    return x";

    auto v = c.parallelize({
        Row(10)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(964004));
}

TEST_F(LoopTest, CodegenTestListF64) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in [1.5, 2.0, 4.0, 100.2]:\n"
                "        x = x + i\n"
                "    return x";

    auto v = c.parallelize({
        Row(1.0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(108.7));
}

TEST_F(LoopTest, CodegenTestListString) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in ['a', 'bc', '%%&&&&&&', 'defg', '100', '']:\n"
                "        x = x + i\n"
                "    return x";

    auto v = c.parallelize({
        Row("A"),
        Row("")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(v[0], std::string("Aabc%%&&&&&&defg100"));
    EXPECT_EQ(v[1], std::string("abc%%&&&&&&defg100"));
}

TEST_F(LoopTest, CodegenTestListDict) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for a in [{0:1, 1:2}, {0:1, 1:2, 2:3}, {1:2, 2:3, 3:4}, {1:2, 2:3, 3:4, 4:5}]:\n"
                "        x = x + len(a)\n"
                "        if len(a) >= 4:\n"
                "            x += a[4]\n"
                "    return x";

    auto v = c.parallelize({
                                   Row(10)
                           }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(27));
}

TEST_F(LoopTest, CodegenTestRange) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in range(4, 0, -1):\n"
                "        x += i\n"
                "    return x";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(10));
}

TEST_F(LoopTest, CodegenTestEmptyString) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func_empty = "def f(x):\n"
                      "    for i in '':\n"
                      "        x = x + i\n"
                      "    return x";

    auto v = c.parallelize({
        Row("should be the same")
    }).map(UDF(func_empty)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], std::string("should be the same"));
}

TEST_F(LoopTest, CodegenTestString) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in '12ab':\n"
                "        x = x + i\n"
                "    return x";

    auto v = c.parallelize({
        Row("test"),
        Row("")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(v[0], std::string("test12ab"));
    EXPECT_EQ(v[1], std::string("12ab"));
}

TEST_F(LoopTest, CodegenTestExprIsId) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    lst = [1, 2, 3, 4]\n"
                "    for i in lst:\n"
                "        x = x + i\n"
                "    return x";

    auto v = c.parallelize({
        Row(10)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(20));
}

TEST_F(LoopTest, CodegenTestEmptyTuple) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in ():\n"
                "        x = x + i\n"
                "    return x";

    auto v = c.parallelize({
        Row(10)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(10));
}

TEST_F(LoopTest, CodegenTestSingleElementTuple) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in (100,):\n"
                "        x = x + str(i)\n"
                "    return x";

    auto v = c.parallelize({
        Row("Num is ")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], std::string("Num is 100"));
}

TEST_F(LoopTest, CodegenTestTupleSameType) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func1 = "def f(x):\n"
                "    for i in (10, 20, 50, 100):\n"
                "        x = x + i\n"
                "    return x";

    auto v1 = c.parallelize({
        Row(0)
    }).map(UDF(func1)).collectAsVector();

    EXPECT_EQ(v1.size(), 1);
    EXPECT_EQ(v1[0], Row(180));

    auto func2 = "def f(x):\n"
                "    for i in ({1:2, 2:4}, {1:5, 2:10}):\n"
                "        x = x + i[2]\n"
                "    return x";

    auto v2 = c.parallelize({
        Row(0)
    }).map(UDF(func2)).collectAsVector();

    EXPECT_EQ(v2.size(), 1);
    EXPECT_EQ(v2[0], Row(14));
}

TEST_F(LoopTest, CodegenTestTupleMixedType) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func1 = "def f(x):\n"
                "    for i in ([2,3], '234'):\n"
                "        x = x + len(i)\n"
                "    return x";

    auto v = c.parallelize({
        Row(2)
    }).map(UDF(func1)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(7));

    auto func2 = "def f(x):\n"
                 "    for i in ('1', 234, 5.6, {}):\n"
                 "        x = x + str(i)\n"
                 "    return x";

    auto v2 = c.parallelize({
        Row("0")
    }).map(UDF(func2)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v2[0], std::string("012345.6{}"));
}

TEST_F(LoopTest, CodegenTestTupleIsId) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func1 = "def f(x):\n"
                 "    val = '1234'\n"
                 "    tpl = (5, ' + ', 100.5, ' - ', val)\n"
                 "    for k in tpl:\n"
                 "        x = x + str(k)\n"
                 "    return x";

    auto v = c.parallelize({
        Row("Expression is ")
    }).map(UDF(func1)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], std::string("Expression is 5 + 100.5 - 1234"));
}

TEST_F(LoopTest, CodegenTestNested) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    lst1 = [1, 2, 3, 4]\n"
                "    lst2 = [2, 3, 4, 5]\n"
                "    for i in lst1:\n"
                "        x = x + i\n"
                "        for j in lst2:\n"
                "            x = x * j\n"
                "    return x";

    auto v = c.parallelize({
        Row(2)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(625579680));

    auto func2 = "def f(x):\n"
                "    t = 0\n"
                "    for i in range(20):\n"
                "        for j in range(40):\n"
                "            t += i + j\n"
                "    x += t\n"
                "    return x";

    auto v2 = c.parallelize({
        Row(200)
    }).map(UDF(func2)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v2[0], Row(23400));
}

TEST_F(LoopTest, CodegenTestNestedMixedType) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(t):\n"
                "    expr1 = ['1', '2, 3, 4', '&?', '!!!!']\n"
                "    expr2 = ([1, 2, 3, 4], '****', (4, 5, 6))\n"
                "    expr3 = range(5)\n"
                "    for i in expr1:\n"
                "        for j in expr2:\n"
                "            for k in expr3:\n"
                "                for p in expr2:\n"
                "                    t += len(i) * len(j) * len(p)\n"
                "    return t";

    auto v = c.parallelize({
        Row(2)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(8472));
}

TEST_F(LoopTest, CodegenTestForWithIf) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    if x > 0:\n"
                "        lst = range(1,5)\n"
                "    else:\n"
                "        lst = range(2)\n"
                "    for i in lst:\n"
                "        x = x + i\n"
                "    return x";

    auto v = c.parallelize({
        Row(1)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(11));
}

TEST_F(LoopTest, CodegenTestForElse) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in [1, 2, 3, 4]:\n"
                "        x = x + i\n"
                "    else:\n"
                "        x *= 2\n"
                "    return x";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(20));
}

TEST_F(LoopTest, CodegenTestForSimpleContinue) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in [1, 2, 3, 4, 5]:\n"
                "        if i == 4:\n"
                "            continue\n"
                "            x -= 10\n"
                "        x = x + i\n"
                "    return x";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(11));
}

TEST_F(LoopTest, CodegenTestForNestedContinue) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in [1, 2, 3, 4, 5]:\n"
                "        for j in [2, 4, 6, 8]:\n"
                "            if i == 4:\n"
                "                continue\n"
                "            if j == 6:\n"
                "                continue\n"
                "            x += i + j\n"
                "        if x == 94:\n"
                "            continue\n"
                "        x += 1\n"
                "    return x";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(94));
}

TEST_F(LoopTest, CodegenTestForSimpleBreak) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func1 = "def f(x):\n"
                "    for num in [1, 2, 3, 4]:\n"
                "        break\n"
                "        x -= 100\n"
                "    return x";

    auto v1 = c.parallelize({
        Row(0)
    }).map(UDF(func1)).collectAsVector();

    EXPECT_EQ(v1.size(), 1);
    EXPECT_EQ(v1[0], Row(0));
}

TEST_F(LoopTest, CodegenTestForNestedBreak) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    lst1 = [-4, -2, 2, 4, 8, 16]\n"
                "    lst2 = [10, 20, 30, 40, 50]\n"
                "    for i in lst1:\n"
                "        for j in lst2:\n"
                "            x += i\n"
                "            if j == 40:\n"
                "                break\n"
                "            x += j\n"
                "        if i == 8:\n"
                "            break\n"
                "        x *= 2\n"
                "    return x";

    auto v = c.parallelize({
        Row(-1000)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(-14364));
}

TEST_F(LoopTest, CodegenTestForBreakElse) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func1 = "def f(x):\n"
                "    for i in range(10):\n"
                "        x += i\n"
                "        if i == 5:\n"
                "            break\n"
                "    else:\n"
                "        x -= 100\n"
                "    return x";

    auto v1 = c.parallelize({
        Row(10)
    }).map(UDF(func1)).collectAsVector();

    EXPECT_EQ(v1.size(), 1);
    EXPECT_EQ(v1[0], Row(25));

    auto func2 = "def f(x):\n"
                 "    for i in 'abcd':\n"
                 "        x += i\n"
                 "        if i == 'e':\n"
                 "            break\n"
                 "    else:\n"
                 "        x += '!'\n"
                 "    return x";

    auto v2 = c.parallelize({
        Row("string: ")
    }).map(UDF(func2)).collectAsVector();

    EXPECT_EQ(v2.size(), 1);
    EXPECT_EQ(v2[0], std::string("string: abcd!"));
}

TEST_F(LoopTest, CodegenTestWhile) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    t = 4\n"
                "    while t > 0:\n"
                "        x += t\n"
                "        t -= 1\n"
                "    return x";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(10));
}

TEST_F(LoopTest, CodegenTestWhileElse) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    t = 'ab'\n"
                "    while len(x) < 10:\n"
                "        x += 'ab'\n"
                "    else:\n"
                "        x += 'END'\n"
                "    return x";

    auto v = c.parallelize({
        Row("")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], std::string("abababababEND"));
}

TEST_F(LoopTest, CodegenTestWhileSimpleContinue) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    t = 100\n"
                "    y = 10\n"
                "    while t >= 10 and y > 1:\n"
                "        t -= 10\n"
                "        y -= 1\n"
                "        x *= y\n"
                "        if t == 50:\n"
                "            continue\n"
                "        x += t\n"
                "    return x";

    auto v = c.parallelize({
        Row(1)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(4452810));
}

TEST_F(LoopTest, CodegenTestWhileNestedContinue) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    t = 10.0\n"
                "    while t >= 0.0:\n"
                "        b = -5.0\n"
                "        while b <= -1.0:\n"
                "            if b == -3.0:\n"
                "                b += 1.0\n"
                "                continue\n"
                "            x += t * b\n"
                "            b += 0.5\n"
                "        if t == 4.0:\n"
                "            t -= 1.0\n"
                "            continue\n"
                "        x += t\n"
                "        t -= 1.0\n"
                "    return x";

    auto v = c.parallelize({
        Row(0.0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(-1131.5));
}

TEST_F(LoopTest, CodegenTestWhileSimpleBreak) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    t = 20\n"
                "    while x > 100:\n"
                "        x -= t\n"
                "        t += 1\n"
                "        if t == 40:\n"
                "            break\n"
                "        x -= t\n"
                "    return x";

    auto v = c.parallelize({
        Row(2000)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(840));
}

TEST_F(LoopTest, CodegenTestWhileNestedBreak) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    t = 0\n"
                "    while x > 0:\n"
                "        t += x\n"
                "        y = -10\n"
                "        while y < 0:\n"
                "            t -= y\n"
                "            y += 1\n"
                "            if t > 100:\n"
                "                break\n"
                "        if x > 100:\n"
                "            break\n"
                "        x += 1\n"
                "    return t";

    auto v = c.parallelize({
        Row(10)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(6088));
}

TEST_F(LoopTest, CodegenTestNestedForWhile) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    t = 10\n"
                "    while t > 0:\n"
                "        for i in range(t):\n"
                "            x += t * t\n"
                "            if x > 2000:\n"
                "                break\n"
                "        t -= 1\n"
                "        if x > 2000:\n"
                "            break\n"
                "    return x";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(2049));
}

TEST_F(LoopTest, CodegenTestForTupleElse) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in ({}, 'val', 1234, 'tuple'):\n"
                "        x = x + str(i)\n"
                "    else:\n"
                "        x += '.'\n"
                "    return x";

    auto v = c.parallelize({
        Row("")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], std::string("{}val1234tuple."));
}

TEST_F(LoopTest, CodegenTestForTupleBreak) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in (2, 2, 3, 3, 5, 5, 7, 7):\n"
                "        x = x + i\n"
                "        if i == 7:\n"
                "            break\n"
                "    return x";

    auto v = c.parallelize({
        Row(100)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(127));
}

TEST_F(LoopTest, CodegenTestForTupleNestedBreak) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in (1, 2, 3, 4, 5, 6, 7):\n"
                "        for j in ([1, 2], '3', '45', 'abcd', 'aaaaa'):\n"
                "            x += i * len(j)\n"
                "            if i == len(j):\n"
                "                break\n"
                "        if x > 200:\n"
                "            break\n"
                "    return x";

    auto v = c.parallelize({
        Row(10)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(249));
}

TEST_F(LoopTest, CodegenTestForTupleContinue) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in ([1, 2], [3, 4, 5, 6], 'str1', 'string', [1]):\n"
                "        if len(i) == 1 or len(i) == 2:\n"
                "            continue\n"
                "        x = x + len(i)\n"
                "    return x";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(14));
}

TEST_F(LoopTest, CodegenTestForTupleNestedContinue) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in ('a:', 'b:', 'c:', '&', 'd:'):\n"
                "        if i == '&':\n"
                "            continue\n"
                "        x += i\n"
                "        for j in (1, 2, 3, 4, 5):\n"
                "            if j == 3:\n"
                "                continue\n"
                "            x += str(j)\n"
                "            if j == 5:\n"
                "                if i == 'd:':\n"
                "                    continue\n"
                "                x += ', '\n"
                "    return x";

    auto v = c.parallelize({
        Row("")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], std::string("a:1245, b:1245, c:1245, d:1245"));
}

TEST_F(LoopTest, CodegenTestForEmptyExprWithElse) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func1 = "def f(x):\n"
                "    for i in []:\n"
                "        break\n"
                "    else:\n"
                "        x -= 1\n"
                "    return x";

    auto v1 = c.parallelize({
        Row(0)
    }).map(UDF(func1)).collectAsVector();

    EXPECT_EQ(v1.size(), 1);
    EXPECT_EQ(v1[0], Row(-1));

    auto func2 = "def f(x):\n"
                "    for i in ():\n"
                "        break\n"
                "    else:\n"
                "        x = 10\n"
                "    return x";

    auto v2 = c.parallelize({
        Row(0)
    }).map(UDF(func2)).collectAsVector();

    EXPECT_EQ(v2.size(), 1);
    EXPECT_EQ(v2[0], Row(10));

    auto func3 = "def f(x):\n"
                 "    for i in range(0):\n"
                 "        break\n"
                 "    else:\n"
                 "        x = 1\n"
                 "    return x";

    auto v3 = c.parallelize({
        Row(0)
    }).map(UDF(func3)).collectAsVector();

    EXPECT_EQ(v3.size(), 1);
    EXPECT_EQ(v3[0], Row(1));
}

TEST_F(LoopTest, CodegenTestGeneralI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = ['1', '2']\n"
                "    for s in a:\n"
                "        for char in 'ab':\n"
                "            for r in (1, '[]', 1234, '.'):\n"
                "                if s + str(r) == '11234':\n"
                "                    break\n"
                "                for k in range(5, 0, -1):\n"
                "                    x += s + char + str(r)\n"
                "                    if str(k) == s:\n"
                "                        continue\n"
                "                    x += str(k)\n"
                "    return x";

    auto v = c.parallelize({
        Row("0")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], std::string("01a151a141a131a121a11a[]51a[]41a[]31a[]21a[]1b151b141b131b121b11b[]51b[]41b["
                                "]31b[]21b[]2a152a142a132a12a112a[]52a[]42a[]32a[]2a[]12a123452a123442a123432a12342a123"
                                "412a.52a.42a.32a.2a.12b152b142b132b12b112b[]52b[]42b[]32b[]2b[]12b123452b123442b123432"
                                "b12342b123412b.52b.42b.32b.2b.1"));
}

TEST_F(LoopTest, CodegenTestGeneralII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = 1\n"
                "    t = 0\n"
                "    res = 0\n"
                "    while True:\n"
                "        if a * a > x:\n"
                "            break\n"
                "        for i in range(a):\n"
                "            while t < a:\n"
                "                res += 1\n"
                "                t += 1\n"
                "            else:\n"
                "                for i in (1, 2, 3, 4, 5):\n"
                "                    res += i\n"
                "                    if i == 4:\n"
                "                        continue\n"
                "                    for j in [2, 4, 6, 8]:\n"
                "                        res += i * j\n"
                "                        if j == 6:\n"
                "                            break\n"
                "                            res *= 2\n"
                "            t = 0\n"
                "        a *= 2\n"
                "    return res";

    auto v = c.parallelize({
        Row(1000)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], 4898);
}

TEST_F(LoopTest, CodegenTestMultiIdTupleI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for a, b in ([10, 20], [30, 40]):\n"
                "        x = x + a\n"
                "        x = x + b\n"
                "    return x";

    auto v = c.parallelize({
        Row(10)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(110));
}

TEST_F(LoopTest, CodegenTestMultiIdTupleII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for a, b, c in (([1], 'ab', (1, 2, 3)), ('tttt', '1!!', [1, 2]), ('1', '22', '333')):\n"
                "        x += len(a) + len(b) + len(c)\n"
                "    return x";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(21));
}

TEST_F(LoopTest, CodegenTestListofTuple) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for a in [(100, 20.5), (0, 40.5)]:\n"
                "        x = x + a[0]\n"
                "        x = x + a[1]\n"
                "    return x";

    auto v = c.parallelize({
        Row(10.0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(171.0));
}

TEST_F(LoopTest, CodegenTestMultiIdListI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for a, b in [(10, 20), (30, 40)]:\n"
                "        x = x + a + b\n"
                "    return x";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(100));
}

TEST_F(LoopTest, CodegenTestMultiIdListII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for a, b, c in [(0, '!!!!', 'a'), (10, '40', 'abcd')]:\n"
                "        x = x + a + len(b) + len(c)\n"
                "    return x";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(21));

    auto func2 = "def f(x):\n"
                "    lst = [([1, 2, 3], 'aabbccdd', 10, '20'), ([4, 5, 6, 7, 8], '1245', 20, '-10')]\n"
                "    for a, b, c, d in lst:\n"
                "        x = x + len(a) + len(b) - c - len(d)\n"
                "    return x";

    auto v2 = c.parallelize({
        Row(0)
    }).map(UDF(func2)).collectAsVector();

    EXPECT_EQ(v2.size(), 1);
    EXPECT_EQ(v2[0], Row(-15));

    auto func3 = "def f(x):\n"
                "    for a, b in [([(1, ), (2, ), (3, )], (3, [1, 2])), ([(5, ), (6, ), (5, )], (7, [0, 0]))]:\n"
                "        x = x + len(a)\n"
                "        x = x + len(b)\n"
                "    return x";

    auto v3 = c.parallelize({
        Row(0)
    }).map(UDF(func3)).collectAsVector();

    EXPECT_EQ(v3.size(), 1);
    EXPECT_EQ(v3[0], Row(10));
}

TEST_F(LoopTest, CodegenTestListAsExprlistI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for [a, b] in ([10, 20], [70, 40], [40, 40], [40, 50], [60, 80], [50, 50]):\n"
                "        if a < 40 or b < 40:\n"
                "            continue\n"
                "        if b > 70:\n"
                "            break\n"
                "        x += a + b\n"
                "    return x";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(280));
}

TEST_F(LoopTest, CodegenTestListAsExprlistII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for [p, q, r] in [(1, 2, 1), (4, 5, 6), (7, 8, 15)]:\n"
                "        if r == p + q:\n"
                "            return x + r\n"
                "    return 0";

    auto v = c.parallelize({
        Row(-5)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(10));
}

TEST_F(LoopTest, CodegenTestLoopInIf) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    if x > 0:\n"
                "        for i in [1, 2, 3, 4]:\n"
                "            x = x + i\n"
                "        return x\n"
                "    else:\n"
                "        while x < 0:\n"
                "            x += 5\n"
                "        return x\n"
                "    return -1";

    auto v = c.parallelize({
        Row(10),
        Row(-8)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 2);
    EXPECT_EQ(v[0], Row(20));
    EXPECT_EQ(v[1], Row(2));
}

TEST_F(LoopTest, CodegenTestExprWithoutParentheses) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func1 = "def f(x):\n"
                 "    for a, b in (1, 2), (3, 4), :\n"
                 "        x = x + a + b\n"
                 "    return x";

    auto v1 = c.parallelize({
        Row(10)
    }).map(UDF(func1)).collectAsVector();

    EXPECT_EQ(v1.size(), 1);
    EXPECT_EQ(v1[0], Row(20));

    auto func2 = "def f(x):\n"
                 "    for i, j, k in ['a', 'b', 'c'], ('d', 'e', 'f'), ['g', 'h', 'i']:\n"
                 "        x = x + i + j + k\n"
                 "    return x";

    auto v2 = c.parallelize({
        Row("")
    }).map(UDF(func2)).collectAsVector();

    EXPECT_EQ(v2.size(), 1);
    EXPECT_EQ(v2[0], std::string("abcdefghi"));
}

TEST_F(LoopTest, CodegenTestLoopWithIterIteratorI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for i in iter([1, 2, 3, 4]):\n"
                "        x += i\n"
                "    return x";

    auto v = c.parallelize({
        Row(1)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(11));
}

TEST_F(LoopTest, CodegenTestLoopWithIterIteratorII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    t = ([(1, 2), (3, 4)], [(5, 6), (7, 8)])\n"
                "    for (i, j) in iter(t):\n"
                "        x += i[0]*i[1]*j[0]*j[1]\n"
                "    return x";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(1704));
}

TEST_F(LoopTest, CodegenTestLoopWithEnumerateIterator) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    y = ''\n"
                "    for index, val in enumerate('abcd', 2):\n"
                "        x += index\n"
                "        y += val\n"
                "    return (x, y)";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(14, "abcd"));
}

TEST_F(LoopTest, CodegenTestLoopWithZipIterator) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    y = ''\n"
                "    for a, b, c in zip([1, 2], 'abcd', (10, 20)):\n"
                "        x += a * c\n"
                "        y += b\n"
                "    return (x, y)";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(50, "ab"));
}

TEST_F(LoopTest, CodegenTestLoopWithIteratorGeneralI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    t = iter([1, 2, 3, 4])\n"
                "    count = 0\n"
                "    for a, b in zip(t, t):\n"
                "        count += 1\n"
                "        x += a + b\n"
                "    return (count, x)";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(2, 10));
}

TEST_F(LoopTest, CodegenTestLoopWithIteratorGeneralII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    t = iter([1, 2, 3, 4])\n"
                "    q = iter('ab')\n"
                "    p = enumerate((10, 20, 30, 40))\n"
                "    for a, b, c in zip(t, q, p):\n"
                "        continue\n"
                "    r1 = next(t)\n"
                "    r2 = next(p)\n"
                "    return (r1, r2)";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(4, Tuple(2, 30)));
}

TEST_F(LoopTest, CodegenTestLoopWithIteratorGeneralIII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    l = iter([1000.5, 2000.5, 3000.0, 4000.0, 5000.5, x])\n"
                "    for index, value in enumerate(l):\n"
                "        if index == 3:\n"
                "            break\n"
                "    b1 = next(l)\n"
                "    b2 = next(l)\n"
                "    return (b1, b2)";

    auto v = c.parallelize({
        Row(6000.0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(5000.5, 6000.0));
}

TEST_F(LoopTest, CodegenTestLoopWithIteratorGeneralIV) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = 'a!b!c!d!A!B!C!D!'\n"
                "    t = enumerate(a, 4)\n"
                "    for i, v in t:\n"
                "        if i == 5:\n"
                "            b1 = v\n"
                "        if i == 10:\n"
                "            b2 = v\n"
                "        if i == 16:\n"
                "            break\n"
                "    else:\n"
                "        next(t)\n"
                "    c = next(t)\n"
                "    return (b1, b2, c)";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row("!", "d", Tuple(17, "!")));
}

TEST_F(LoopTest, CodegenTestLoopOverInputDataI) {
    using namespace tuplex;

    ClosureEnvironment ce;
    ce.importModuleAs("math", "math");

    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    total = 0\n"
                "    n = len(x[1])\n"
                "    for num in x[1]:\n"
                "        total += num\n"
                "    mean_val = total/n\n"
                "    var_denominator = 0.0\n"
                "    for num in x[1]:\n"
                "        var_denominator += (num-mean_val) ** 2\n"
                "    std = math.sqrt(var_denominator/n)\n"
                "    return std\n";

    auto v = c.parallelize({
        Row(10, List(1, 2, 3, 4))
    }).map(UDF(func, "", ce)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_DOUBLE_EQ(v[0].getDouble(0), sqrt(1.25));
}

TEST_F(LoopTest, CodegenTestLoopOverInputDataII) {
using namespace tuplex;
Context c(microTestOptions());

auto func = "def f(x):\n"
            "    s = ''\n"
            "    for i in x:\n"
            "        s += i\n"
            "    return s";

auto v = c.parallelize({
    Row(List("a", "bc", "def", "ghij", "k"))
}).map(UDF(func)).collectAsVector();

EXPECT_EQ(v.size(), 1);
EXPECT_EQ(v[0], Row("abcdefghijk"));
}