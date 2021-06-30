#!/usr/bin/env python3
#----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 1/1/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
#----------------------------------------------------------------------------------------------------------------------#

import unittest
from tuplex import *

class TestTuples(unittest.TestCase):

    def setUp(self):
        self.conf = {"webui.enable" : False, "driverMemory" : "16MB", "partitionSize" : "256KB"}

    def testEmptyTupleI(self):
        c = Context(self.conf)
        res = c.parallelize([1, 2, 4]).map(lambda x: ()).collect()
        assert res == [(), (), ()]

    def testEmptyTupleII(self):
        c = Context(self.conf)
        res = c.parallelize([1, 2, 4]).map(lambda x: ()).collect()
        assert res == [(), (), ()]

    def testNestedEmptyTupleI(self):
        c = Context(self.conf)
        res = c.parallelize([('hello', '', (), ('world',  ()))]).collect()
        assert res == [('hello', '', (), ('world',  ()))]

    def testNestedTuple(self):
        c = Context(self.conf)
        res = c.parallelize([(10, 20), (20, 40)]).map(lambda x: (x, x)).collect()
        assert res == [((10, 20), (10, 20)), ((20, 40), (20, 40))]

    def testTupleMixed(self):
        c = Context(self.conf)
        res = c.parallelize([1, 2, 4]).map(lambda x: (x, x+1, (), x * x)).collect()
        assert res == [(1, 2, (), 1), (2, 3, (), 4), (4, 5, (), 16)]

    def testTupleWithStrings(self):
        c = Context(self.conf)
        res = c.parallelize([(10, 'hello'), (20, 'world')]).map(lambda x: (x, 'test')).collect()
        assert res == [((10, 'hello'), 'test'), ((20, 'world'), 'test')]

    def testTupleMultiParamUnpacking(self):
        c = Context(self.conf)
        res = c.parallelize([(10, 20), (40, 50)]).map(lambda a, b: a + b).collect()
        assert res == [30, 90]

    def testTupleMultiParamUnpackingII(self):
        c = Context(self.conf)
        res = c.parallelize([(10, (30, 40), 20), (40, (10, 20), 50)]).map(lambda a, b, c: b).map(lambda a, b: a + b).collect()
        assert res == [70, 30]

    def testTupleMultiLine(self):
        # make sure code extraction works over multiple lines
        c = Context(self.conf)
        res = c.parallelize([1, 3, 5]).map(lambda x: (x,
                                                      x+ 1)) \
        .collect()
        assert res == [(1, 2), (3, 4), (5, 6)]

    def testTupleSlice(self):
        c = Context(self.conf)

        testsets = [[(1, 2, 3, 4, 5, 6), (4, 5, 6, 7, 10, 11), (-10, -12, 0, -1, 2, 4)],
                    [((), ("hello",), 123, "oh no", (1, 2)), ((), ("goodbye",), 123, "yes", (-10, 2)),
                     ((), ("foobar",), 1443, "no", (100, 0))]]

        funcs = [lambda x: x[-10:], lambda x: x[:-10], lambda x: x[::-10],
                 lambda x: x[-2:], lambda x: x[:-2], lambda x: x[::-2],
                 lambda x: x[3:], lambda x: x[:3], lambda x: x[::3],
                 lambda x: x[1:], lambda x: x[:1], lambda x: x[::1],
                 lambda x: x[10:], lambda x: x[:10], lambda x: x[::10],
                 lambda x: x[-10:10:], lambda x: x[-10::10], lambda x: x[:-10:10],
                 lambda x: x[-10:-2:], lambda x: x[-10::-2], lambda x: x[:-10:-2],
                 lambda x: x[10:-2:], lambda x: x[10::-2], lambda x: x[:10:-2],
                 lambda x: x[-10:2:], lambda x: x[-10::2], lambda x: x[:-10:2],
                 lambda x: x[2:-10:], lambda x: x[2::-10], lambda x: x[:2:-10],
                 lambda x: x[-2:10:], lambda x: x[-2::10], lambda x: x[:-2:10],
                 lambda x: x[2:10:], lambda x: x[2::10], lambda x: x[:2:10],
                 lambda x: x[-2:-1:], lambda x: x[-2::-1], lambda x: x[:-2:-1],
                 lambda x: x[-3:4:], lambda x: x[-3::4], lambda x: x[:-3:4],
                 lambda x: x[-10:10:2], lambda x: x[-10:10:2], lambda x: x[-10:10:2],
                 lambda x: x[1:10:2], lambda x: x[1:10:2], lambda x: x[1:10:2],
                 lambda x: x[1:4:2], lambda x: x[1:4:2], lambda x: x[1:4:2],
                 lambda x: x[4:1:-2], lambda x: x[4:1:-2], lambda x: x[4:1:-2]]

        for testset in testsets:
            for func in funcs:
                res = c.parallelize(testset).map(func).collect()
                assert res == list(map(func, testset))


    def test_tupleExpr(self):

        c = Context(self.conf)

        def f(x):
            return x,

        res = c.parallelize([1, 2, 3]).map(f).collect()

        assert res == [(1,), (2,), (3,)]

        def swapI(a, b):
            return b, a
        res = c.parallelize([('a', 1), ('b', 2)]).map(swapI).collect()
        assert res == [(1, 'a'), (2, 'b')]

        def swapII(x):
            b, a = x
            y = a, b
            return y

        res = c.parallelize([('a', 1), ('b', 2)]).map(swapII).collect()
        assert res == [(1, 'a'), (2, 'b')]

        def swapIII(x):
            a = x[0]
            b = x[1]
            b, a = a, b
            return a, b

        res = c.parallelize([('a', 1), ('b', 2)]).map(swapIII).collect()
        assert res == [(1, 'a'), (2, 'b')]