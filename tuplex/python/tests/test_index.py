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
from .helper import test_options

class TestTuples(unittest.TestCase):

    def setUp(self):
        self.conf = test_options()
        self.conf.update({"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"})

    def testIndexI(self):
        c = Context(self.conf)
        res = c.parallelize([(1, 2), (2, 4), (4, 8)]).map(lambda x: x[0]).collect()
        assert res == [1, 2, 4]

        res = c.parallelize([(1, 2), (2, 4), (4, 8)]).map(lambda x: x[1]).collect()
        assert res == [2, 4, 8]

    def testIndexNegative(self):
        c = Context(self.conf)
        res = c.parallelize([(1, 2), (2, 4), (4, 8)]).map(lambda x: x[-1]).collect()
        assert res == [2, 4, 8]

        res = c.parallelize([(1, 2), (2, 4), (4, 8)]).map(lambda x: x[-2]).collect()
        assert res == [1, 2, 4]

    def testIndexMulti(self):
        c = Context(self.conf)
        res = c.parallelize([(0, ('hello',)), (0, ('world',))]).map(lambda x: x[1][x[0]]).collect()
        assert res == ['hello', 'world']

    # because the tuple here has the same element types, this works & can be resolved during compile time.
    def testIndexIntra(self):
        c = Context(self.conf)
        res = c.parallelize([(0, 1, 2, 3), (1, 1, 2, 3), (2, 1, 2, 3), (3, 1, 2, 3)]).map(lambda x: x[x[0]]).collect()
        assert res == [0, 1, 2, 3]

class TestStrings(unittest.TestCase):
    def setUp(self):
        self.conf = {"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"}

    def testValidStringIndexing(self):
        c = Context(self.conf)
        res = c.parallelize(['abcdef', 'ghijkl', 'mnop']).map(lambda x: x[0]).collect()
        assert res == ['a', 'g', 'm']

        res = c.parallelize(['abcdef', 'ghijkl', 'mnop']).map(lambda x: x[2]).collect()
        assert res == ['c', 'i', 'o']

        res = c.parallelize(['abcdef', 'ghijkl', 'mnop']).map(lambda x: x[-1]).collect()
        assert res == ['f', 'l', 'p']

        res = c.parallelize(['abcdef', 'ghijkl', 'mnop']).map(lambda x: x[-3]).collect()
        assert res == ['d', 'j', 'n']

    def testInvalidStringIndexing(self):
        # should raise exceptions, however returned normal case is simply without error rows
        c = Context(self.conf)
        res = c.parallelize(['hello', 'world!', 'thisisatest']).map(lambda x: x[5]).collect()
        assert res == ['!', 's']

        res = c.parallelize(['hello', 'world!', 'thisisatest']).map(lambda x: x[-10]).collect()
        assert res == ['h']