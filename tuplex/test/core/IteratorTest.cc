#include "gtest/gtest.h"
#include <Context.h>
#include "TestUtils.h"

class IteratorTest : public PyTest {};

TEST_F(IteratorTest, CodegenTestListIteratorI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = iter([x, 20, 30, 40])\n"
                "    b1 = next(a)\n"
                "    b2 = next(a)\n"
                "    b3 = next(a, 100)\n"
                "    b4 = next(a, 100)\n"
                "    b5 = next(a, 100)\n"
                "    b6 = next(a, 100)\n"
                "    b7 = next(a, -100)\n"
                "    b8 = next(a, -100)\n"
                "    return (b1, b2, b3, b4, b5, b6, b7, b8)";

    auto v = c.parallelize({
        Row(10)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(10, 20, 30, 40, 100, 100, -100, -100));
}

TEST_F(IteratorTest, CodegenTestListIteratorII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = iter([-10.5, 0.5, 1.1, 100.0])\n"
                "    b = next(a)\n"
                "    b += next(a)\n"
                "    b += next(a, 0.1)\n"
                "    b += next(a, 0.1)\n"
                "    b += next(a, 0.1)\n"
                "    return b";

    auto v = c.parallelize({
        Row(10)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(-10.5 + 0.5 + 1.1 + 100.0 + 0.1));
}

TEST_F(IteratorTest, CodegenTestListIteratorIII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = [(1, 2), (3, 4)]\n"
                "    b = iter(a)\n"
                "    c1 = next(b)\n"
                "    c2 = next(b, (100, 100))\n"
                "    c3 = next(b, (100, 100))\n"
                "    c4 = next(b, (200, 200))\n"
                "    return (c1, c2, c3, c4)";

    auto v = c.parallelize({
        Row(1, 2)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(Tuple(1, 2), Tuple(3, 4), Tuple(100, 100), Tuple(200, 200)));
}

TEST_F(IteratorTest, CodegenTestListIteratorIV) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = [x, 'xyz', 'pq', 't', '00', '11']\n"
                "    b = iter(a)\n"
                "    c1 = next(b)\n"
                "    c2 = next(b)\n"
                "    c3 = next(b)\n"
                "    c4 = next(b)\n"
                "    c5 = next(b)\n"
                "    c6 = next(b, 'end')\n"
                "    c7 = next(b, 'end')\n"
                "    return (c1, c2, c3, c4, c5, c6, c7)";

    auto v = c.parallelize({
        Row("abcd")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row("abcd", "xyz", "pq", "t", "00", "11", "end"));
}

TEST_F(IteratorTest, CodegenTestListIteratorV) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = [{1:2, 3:4}, {5:6, 7:8}]\n"
                "    b = iter(a)\n"
                "    c1 = next(b)\n"
                "    c2 = next(b)\n"
                "    return (c1, c2)";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0].toPythonString(), "({1:2,3:4},{5:6,7:8})");
}

TEST_F(IteratorTest, CodegenTestStringIterator) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = iter(x)\n"
                "    b1 = next(a)\n"
                "    b2 = next(a)\n"
                "    b3 = next(a, 'xx')\n"
                "    b4 = next(a, 'xx')\n"
                "    b5 = next(a, 'xx')\n"
                "    b6 = next(a, 'xx')\n"
                "    b7 = next(a, 'z')\n"
                "    return (b1, b2, b3, b4, b5, b6, b7)";

    auto v = c.parallelize({
        Row("qwert")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row("q", "w", "e", "r", "t", "xx", "z"));
}

TEST_F(IteratorTest, CodegenTestRangeIteratorI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = iter(range(x, 10, 3))\n"
                "    b1 = next(a)\n"
                "    b2 = next(a)\n"
                "    b3 = next(a, -x)\n"
                "    b4 = next(a, -x)\n"
                "    b5 = next(a, -2*x)\n"
                "    return (b1, b2, b3, b4, b5)";

    auto v = c.parallelize({
        Row(2)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(2, 5, 8, -2, -4));
}

TEST_F(IteratorTest, CodegenTestRangeIteratorII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = iter(range(x, -25, -5))\n"
                "    b1 = next(a)\n"
                "    b2 = next(a)\n"
                "    b3 = next(a)\n"
                "    b4 = next(a)\n"
                "    b5 = next(a, 100)\n"
                "    b6 = next(a, 100)\n"
                "    b7 = next(a, 100)\n"
                "    return (b1, b2, b3, b4, b5, b6, b7)";

    auto v = c.parallelize({
        Row(-4)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(-4, -9, -14, -19, -24, 100, 100));
}

TEST_F(IteratorTest, CodegenTestTupleIteratorI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = iter(x)\n"
                "    b1 = next(a)\n"
                "    b2 = next(a)\n"
                "    b3 = next(a, -1)\n"
                "    b4 = next(a, -1)\n"
                "    b5 = next(a, -1)\n"
                "    b6 = next(a, -2)\n"
                "    return (b1, b2, b3, b4, b5, b6)";

    auto v = c.parallelize({
        Row(1, 2, 3, 4)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(1, 2, 3, 4, -1, -2));
}

TEST_F(IteratorTest, CodegenTestTupleIteratorII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = iter(x)\n"
                "    b1 = next(a)\n"
                "    b2 = next(a)\n"
                "    b3 = next(a, 'x')\n"
                "    b4 = next(a, 'x')\n"
                "    b5 = next(a, 'x')\n"
                "    b6 = next(a, 'x')\n"
                "    return (b1, b2, b3, b4, b5, b6)";

    auto v = c.parallelize({
        Row("a", "b", "c", "d")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row("a", "b", "c", "d", "x", "x"));
}

TEST_F(IteratorTest, CodegenTestTupleIteratorIII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = iter(x)\n"
                "    b1 = next(a)\n"
                "    b2 = next(a)\n"
                "    b3 = next(a, [5, 6])\n"
                "    b4 = next(a, [7, 8])\n"
                "    return (b1, b2, b3, b4)";

    auto v = c.parallelize({
        Row(List(1, 2), List(3, 4))
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(List(1, 2), List(3, 4), List(5, 6), List(7, 8)));
}

TEST_F(IteratorTest, CodegenTestZipIteratorI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = zip([1, 2], 'abcd', (3, 4), [('p', 'q'), ('x', 'y')])\n"
                "    b1 = next(a)\n"
                "    b2 = next(a)\n"
                "    b3 = next(a, (-1, 'None', -1, ('!', '!')))\n"
                "    return (b1, b2, b3)";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(Tuple(1, "a", 3, Tuple("p", "q")), Tuple(2, "b", 4, Tuple("x", "y")), Tuple(-1, "None", -1, Tuple("!", "!"))));
}

TEST_F(IteratorTest, CodegenTestZipIteratorII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = zip([('a', 'b'), ('c', 'd')], [('ee', 'ff'), ('gg', 'hh')], [('iii', 'jjj'), ('kkk', 'lll')])\n"
                "    b1 = next(a)\n"
                "    b2 = next(a)\n"
                "    return (b1, b2)";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(Tuple(Tuple("a", "b"), Tuple("ee", "ff"), Tuple("iii", "jjj")), Tuple(Tuple("c", "d"), Tuple("gg", "hh"), Tuple("kkk", "lll"))));
}

TEST_F(IteratorTest, CodegenTestEnumerateIteratorI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = enumerate(x)\n"
                "    b1 = next(a)\n"
                "    b2 = next(a)\n"
                "    b3 = next(a, (-1, 'x'))\n"
                "    return (b1, b2, b3)";

    auto v = c.parallelize({
        Row(List("a", "b"))
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(Tuple(0, "a"), Tuple(1, "b"), Tuple(-1, "x")));
}

TEST_F(IteratorTest, CodegenTestEnumerateIteratorII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = enumerate(x)\n"
                "    b1 = next(a)\n"
                "    b2 = next(a)\n"
                "    b3 = next(a)\n"
                "    b4 = next(a, (3, 'A'))\n"
                "    return (b1, b2, b3, b4)";

    auto v = c.parallelize({
        Row("xyz")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(Tuple(0, "x"), Tuple(1, "y"), Tuple(2, "z"), Tuple(3, "A")));
}

TEST_F(IteratorTest, CodegenTestEmptyIteratorI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = iter([])\n"
                "    b1 = next(a, 'None')\n"
                "    b2 = next(a, 0)\n"
                "    return (b1, b2)";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row("None", 0));
}

TEST_F(IteratorTest, CodegenTestEmptyIteratorII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = iter(())\n"
                "    b1 = next(a, '#')\n"
                "    b2 = next(a, -1)\n"
                "    return (b1, b2)";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row("#", -1));
}

TEST_F(IteratorTest, CodegenTestEmptyIteratorIII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = zip([], 'a')\n"
                "    b1 = next(a, 'None')\n"
                "    b2 = next(a, 0)\n"
                "    return (b1, b2)";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row("None", 0));
}

TEST_F(IteratorTest, CodegenTestNestedIteratorI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = enumerate(iter(enumerate(iter([-1, -2, -3, -4]))))\n"
                "    b = zip(a, 'abcd', enumerate(zip([1, 2], [3, 4])), zip(('A', 'B'), ('C', 'D')))\n"
                "    c = enumerate(b, 10)\n"
                "    d = iter(zip(iter(c), a))\n"
                "    e1 = next(d)\n"
                "    e2 = next(d)\n"
                "    return (e1, e2)";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0].toPythonString(), "(((10,((0,(0,-1)),'a',(0,(1,3)),('A','C'))),(1,(1,-2))),((11,((2,(2,-3)),'b',(1,(2,4)),('B','D'))),(3,(3,-4))))");
}

TEST_F(IteratorTest, CodegenTestNestedIteratorII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = [0, 1, 2, 3, 4, 5, 6]\n"
                "    b = (10, 20, 30, 40, 50, 60)\n"
                "    c = 'ABCDEFG'\n"
                "    d = ('a', 'b', 'c', 'd', 'e', 'f')\n"
                "    e = zip(a, b, c, d)\n"
                "    f = zip(e, e, a, b)\n"
                "    g = zip(f, e, c, d)\n"
                "    h1 = next(g)\n"
                "    h2 = next(g)\n"
                "    return (h1, h2)";

    auto v = c.parallelize({
        Row(0)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0].toPythonString(), "((((0,10,'A','a'),(1,20,'B','b'),0,10),(2,30,'C','c'),'A','a'),(((3,40,'D','d'),(4,50,'E','e'),1,20),(5,60,'F','f'),'B','b'))");
}

TEST_F(IteratorTest, CodegenTestIfWithIteratorI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    if x > 0:\n"
                "        b = iter([1, 2])\n"
                "        c = next(b)\n"
                "        return c\n"
                "    return x\n";

    auto v = c.parallelize({
        Row(10)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], 1);
}

TEST_F(IteratorTest, CodegenTestIfWithIteratorII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = iter(x)\n"
                "    while True:\n"
                "        if next(a) == 3:\n"
                "            break\n"
                "        else:\n"
                "            next(a)\n"
                "    return next(a)\n";

    auto v = c.parallelize({
        Row(List(1, 2, 3, 4))
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(4));
}