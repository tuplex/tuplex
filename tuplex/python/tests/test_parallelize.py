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


# all of these below should be executed with faster, optimized serialization code
class TestFastParallelize(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        self.conf = {"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"}
        super(TestFastParallelize, self).__init__(*args, **kwargs)

    def testI64(self):
        c = Context(self.conf)

        ref = [-20, -90, 0, 42, 3, 1, 2, 3, 4, 5]
        res = c.parallelize(ref).collect()

        assert res == ref

    def testF64(self):
        c = Context(self.conf)

        ref = [-3.141, -90.0, -8.2, 42.0, 3.0, 1.0, 2.0, 3.0, 4.0, 5.0]
        res = c.parallelize(ref).collect()

        assert res == ref

    def testString(self):
        c = Context(self.conf)

        ref = ['', 'Hello', 'world', 'how', 'are', 'you?']
        res = c.parallelize(ref).collect()

        assert res == ref


    def testI64Tuple(self):
        c = Context(self.conf)

        ref = [(-20, 3, 1), (10, 3, 1), (2, 3, 4), (4, 5, 6)]
        res = c.parallelize(ref).collect()

        assert res == ref

    def testF64Tuple(self):
        c = Context(self.conf)

        ref = [(-20.0, 3., 1.), (10.2, 3.3, 1.1), (2.6, 3.5, .4), (.4, .5, .6)]
        res = c.parallelize(ref).collect()

        assert res == ref

    def testStringTuple(self):
        c = Context(self.conf)

        ref = [('', 'Hello'), ('world', 'how'), ('are', 'you?')]
        res = c.parallelize(ref).collect()

        assert res == ref

    def testOptionTypeI(self):
        c = Context(self.conf)
        ref = [1, None, 2, 3, None]
        res = c.parallelize(ref).collect()

        assert res == ref

    def testOptionTypeII(self):
        c = Context(self.conf)
        ref = [1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29]
        res = c.parallelize(ref).map(lambda x: x if x > 10 else None).collect()

        assert res == [None, None, None, None, None, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29]

    def testTupleOptionTypeI(self):
        c = Context(self.conf)
        ref = [(1.0, '2', 3, '4', 5, 6, True, 8, 9, None), (None, '2', 3, None, 5, 6, True, 8, 9, None), (1.0, '2', 3, '4', None, 6, None, 8, 9, None)]
        res = c.parallelize(ref).collect()

        assert res == ref

    def testAutoUnpack(self):
        c = Context(self.conf)
        input = [{"a":1,"b":2,"c":3},{"a":4,"b":5,"c":6},{"a":7,"b":8,"c":9}]
        output = c.parallelize(input).collect()
        self.assertEqual([(1, 3, 2), (4, 6, 5), (7, 9, 8)], output)

        input = [{"a":1,"b":2,"c":3},{"a":4,"b":5,"c":6},{"a":7,"b":8,"c":9}]
        output = c.parallelize(input, auto_unpack=False).collect()
        self.assertEqual(input, output)

        input = [{"a":1,"b":2,"c":3},{"a":4,"b":5,"c":6},{"a":7,"b":8,"c":9}, {"a": 1, "b":2}, {}]
        output = c.parallelize(input).collect()
        self.assertEqual([(1, 3, 2), (4, 6, 5), (7, 9, 8), (1, None, 2), (None, None, None)], output)

        input = [{"a":1,"b":2,"c":3},{"d":4,"e":5,"f":6}]
        output = c.parallelize(input).collect()
        self.assertEqual([(1, 2, None, None, 3, None), (None, None, 4, 5, None, 6)], output)

    def testTupleOptionTypeII(self):
        c = Context(self.conf)
        ref = [(1.0, '2'), (None, '2'), (1.0, '2')]
        res = c.parallelize(ref).collect()

        assert res == ref

    def testNoneType(self):
        c = Context(self.conf)
        ref = [None, None]
        res = c.parallelize(ref).collect()

        assert res == ref