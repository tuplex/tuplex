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

TEST_F(IteratorTest, CodegenTestListIteratorVI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = [True, True, False, False, True, x]\n"
                "    b = iter(a)\n"
                "    total = 0\n"
                "    for i in b:\n"
                "        total += i\n"
                "    return total";

    auto v = c.parallelize({
        Row(true)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], 4);
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

TEST_F(IteratorTest, CodegenTestListReverseIterator) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = reversed(x)\n"
                "    b1 = next(a)\n"
                "    b2 = next(a)\n"
                "    b3 = next(a)\n"
                "    b4 = next(a)\n"
                "    b5 = next(a, 0)\n"
                "    b6 = next(a, -1)\n"
                "    return (b1, b2, b3, b4, b5, b6)";

    auto v = c.parallelize({
        Row(List(1, 2, 3, 4))
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(4, 3, 2, 1, 0, -1));
}

TEST_F(IteratorTest, CodegenTestTupleReverseIterator) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = reversed(x)\n"
                "    b = next(a)\n"
                "    for i in a:\n"
                "        b += i\n"
                "    b += next(a, 'abc')\n"
                "    return b";

    auto v = c.parallelize({
        Row("yz", "x", "uvw", "rst", "q")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row("qrstuvwxyzabc"));
}

TEST_F(IteratorTest, CodegenTestStringReverseIterator) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = reversed(x)\n"
                "    b1 = next(a)\n"
                "    b2 = next(a)\n"
                "    b2 += next(a)\n"
                "    b3 = ''\n"
                "    for s in a:\n"
                "        b3 += s\n"
                "    b4 = next(a, 'end')\n"
                "    return (b1, b2, b3, b4)";

    auto v = c.parallelize({
        Row("hgfedcba")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row("a", "bc", "defgh", "end"));
}

TEST_F(IteratorTest, CodegenTestRangeReverseIteratorI) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    r1 = range(-100, x, 17)\n"
                "    a = reversed(r1)\n"
                "    total1 = 0\n"
                "    while True:\n"
                "        b = next(a)\n"
                "        if b < -20:\n"
                "            break\n"
                "        total1 += b\n"
                "    total2 = 0\n"
                "    r2 = reversed(range(-x, -25000, -171))\n"
                "    for i in r2:\n"
                "        total2 += i\n"
                "    total2 += next(r2, total1)\n"
                "    return total1, total2\n";

    auto v = c.parallelize({
        Row(1000)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(29190, -1799580));
}

TEST_F(IteratorTest, CodegenTestRangeReverseIteratorII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    r1 = reversed(range(4, 15, 2))\n"
                "    a1 = next(r1)\n"
                "    a2 = next(r1)\n"
                "    a3 = next(r1)\n"
                "    a4 = next(r1)\n"
                "    a5 = next(r1)\n"
                "    a6 = next(r1)\n"
                "    a7 = next(r1, -1)\n"
                "    r2 = reversed(range(8, 6, -1))\n"
                "    b1 = next(r2)\n"
                "    b2 = next(r2, -1)\n"
                "    b3 = next(r2, -2)\n"
                "    b4 = next(r2, -3)\n"
                "    return (a1, a2, a3, a4, a5, a6, a7, b1, b2, b3, b4)\n";

    auto v = c.parallelize({
        Row(1000)
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(14, 12, 10, 8, 6, 4, -1, 7, 8, -2, -3));
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

TEST_F(IteratorTest, CodegenTestEmptyIteratorIV) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = reversed(x)\n"
                "    b1 = next(a, -1)\n"
                "    b2 = next(a, 'empty')\n"
                "    return (b1, b2)";

    auto v = c.parallelize({
        Row(Field::empty_list())
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(-1, "empty"));
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

TEST_F(IteratorTest, CodegenTestNestedIteratorIII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    a = reversed(range(4, 15, 2))\n"
                "    b = enumerate(reversed(['a', 'b', 'c', 'd']))\n"
                "    c = iter((1, 2, 3, 4))\n"
                "    d = zip(a, b, c, reversed(x))\n"
                "    e1 = next(d)\n"
                "    e2 = next(d)\n"
                "    e3 = next(d)\n"
                "    e4 = next(d)\n"
                "    e5 = next(d, (0, (0, 'EMPTY'), 0, 'EMPTY'))\n"
                "    return (e1, e2, e3, e4, e5)";

    auto v = c.parallelize({
        Row("!@#$%^&*")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0].toPythonString(), "((14,(0,'d'),1,'*'),(12,(1,'c'),2,'&'),(10,(2,'b'),3,'^'),(8,(3,'a'),4,'%'),(0,(0,'EMPTY'),0,'EMPTY'))");
}

TEST_F(IteratorTest, CodegenTestNestedIteratorIV) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    for index, k in enumerate(zip(reversed(range(x[0], x[1], x[2])), enumerate(reversed('abcdefghijklmnopqrstuvwxyz')), range(2022, -100, -105))):\n"
                "        if index == 0:\n"
                "            index0 = k\n"
                "        if index == 5:\n"
                "            index5 = k\n"
                "        if index == 10:\n"
                "            index10 = k\n"
                "        if index == 15:\n"
                "            index15 = k\n"
                "        if index == 20:\n"
                "            index20 = k\n"
                "    return (index0, index5, index10, index15, index20)";

    auto v = c.parallelize({
        Row(List(-1111, 2021, 21))
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0].toPythonString(), "((2018,(0,'z'),2022),(1913,(5,'u'),1497),(1808,(10,'p'),972),(1703,(15,'k'),447),(1598,(20,'f'),-78))");
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

TEST_F(IteratorTest, CodegenTestIfWithIteratorIII) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    t = iter(x[1])\n"
                "    if x[0] > 0:\n"
                "        if next(t) > 0:\n"
                "            next(t)\n"
                "            next(t)\n"
                "        next(t)\n"
                "        y = next(t)\n"
                "    else:\n"
                "        y = -10\n"
                "    return y\n";

    auto v = c.parallelize({
        Row(1, List(10, 20, 30, 40, 50, 60))
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row(50));
}

TEST_F(IteratorTest, CodegenTestIfWithIteratorIV) {
    using namespace tuplex;
    Context c(microTestOptions());

    auto func = "def f(x):\n"
                "    t = enumerate(x)\n"
                "    while True:\n"
                "        if next(t)[0] < 4:\n"
                "            continue\n"
                "        else:\n"
                "            break\n"
                "    if next(t)[1] == 'f':\n"
                "        if next(t)[1] == 'g':\n"
                "            return next(t)[1]\n"
                "        else:\n"
                "            return 'fail'\n"
                "    else:\n"
                "        return 'fail'\n";

    auto v = c.parallelize({
        Row("a", "b", "c", "d", "e", "f", "g", "h")
    }).map(UDF(func)).collectAsVector();

    EXPECT_EQ(v.size(), 1);
    EXPECT_EQ(v[0], Row("h"));
}