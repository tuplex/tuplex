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


class TestLogical(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        self.conf = {"webui.enable": False, "driverMemory": "64MB", "executorMemory": "2MB", "partitionSize": "128KB"}
        super(TestLogical, self).__init__(*args, **kwargs)

    def testAnd(self):
        c = Context(self.conf)
        res = c.parallelize([True, False]).map(lambda x: x and True).collect()
        assert res == [True, False]

        res = c.parallelize([True, False]).map(lambda x: x and False).collect()
        assert res == [False, False]

        del c

    def testOr(self):
        c = Context(self.conf)

        res = c.parallelize([True, False]).map(lambda x: x or True).collect()
        assert res == [True, True]

        res = c.parallelize([True, False]).map(lambda x: x or False).collect()
        assert res == [True, False]

        del c


    # NOTE: AND op has higher precedence than OR op in Python
    def testAndOr(self):
        c = Context(self.conf)

        res = c.parallelize([True, False]).map(lambda x: x and True or True).collect()
        assert res  == [True, True]

        res = c.parallelize([True, False]).map(lambda x: x and True or False).collect()
        assert res  == [True, False]

        res = c.parallelize([True, False]).map(lambda x: x and False or True).collect()
        assert res  == [True, True]

        res = c.parallelize([True, False]).map(lambda x: x and False or False).collect()
        assert res  == [False, False]

        res = c.parallelize([True, False]).map(lambda x: x or True and True).collect()
        assert res  == [True, True]

        res = c.parallelize([True, False]).map(lambda x: x or True and False).collect()
        assert res  == [True, False]

        res = c.parallelize([True, False]).map(lambda x: x or False and True).collect()
        assert res  == [True, False]

        res = c.parallelize([True, False]).map(lambda x: x or False and False).collect()
        assert res  == [True, False]

        del c

    def testBitwise(self):
        c = Context(self.conf)

        # booleans
        res = c.parallelize([(False, False), (False, True),
                             (True, False), (True, True)]).map(lambda a, b: a & b).collect()
        self.assertEqual(res, [False & False, False & True, True & False, True & True])

        res = c.parallelize([(False, False), (False, True),
                             (True, False), (True, True)]).map(lambda a, b: a | b).collect()
        self.assertEqual(res, [False | False, False | True, True | False, True | True])

        res = c.parallelize([(False, False), (False, True),
                             (True, False), (True, True)]).map(lambda a, b: a ^ b).collect()
        self.assertEqual(res, [False ^ False, False ^ True, True ^ False, True ^ True])

        # integers
        res = c.parallelize([(-1, 2), (0, 0),
                             (3, 34567), (-20, -42)]).map(lambda a, b: a & b).collect()
        self.assertEqual(res, [-1 & 2, 0 & 0, 3 & 34567, -20 & -42])

        res = c.parallelize([(-1, 2), (0, 0),
                             (3, 34567), (-20, -42)]).map(lambda a, b: a | b).collect()
        self.assertEqual(res, [-1 | 2, 0 | 0, 3 | 34567, -20 | -42])

        res = c.parallelize([(-1, 2), (0, 0),
                             (3, 34567), (-20, -42)]).map(lambda a, b: a ^ b).collect()
        self.assertEqual(res, [-1 ^ 2, 0 ^ 0, 3 ^ 34567, -20 ^ -42])

    def testWithRelationalOps(self):
        c = Context(self.conf)

        # and
        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x < 2 and x > 2).collect()
        assert res  == [False, False, False, False, False]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x <= 2 and x >= 2).collect()
        assert res  == [False, False, False, True, False]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x < 2 and x >= 2).collect()
        assert res  == [False, False, False, False, False]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x <= 2 and x > 2).collect()
        assert res  == [False, False, False, False, False]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x > 0 and x > 1).collect()
        assert res  == [False, False, False, True, True]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x < 0 and x < 1).collect()
        assert res  == [True, False, False, False, False]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x > 0 and x < 2).collect()
        assert res  == [False, False, True, False, False]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x >= 0 and x <= 2).collect()
        assert res  == [False, True, True, True, False]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x > 0 and x < 2).collect()
        assert res  == [False, False, True, False, False]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: 0 < x < 2).collect()
        assert res  == [False, False, True, False, False]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: 0 > x >= -1).collect()
        assert res  == [True, False, False, False, False]



        # or
        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x < 2 or x > 2).collect()
        assert res  == [True, True, True, False, True]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x <= 2 or x >= 2).collect()
        assert res  == [True, True, True, True, True]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x < 2 or x >= 2).collect()
        assert res  == [True, True, True, True, True]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x <= 2 or x > 2).collect()
        assert res  == [True, True, True, True, True]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x > 0 or x > 1).collect()
        assert res  == [False, False, True, True, True]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x < 0 or x < 1).collect()
        assert res  == [True, True, False, False, False]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x > 0 or x < 2).collect()
        assert res  == [True, True, True, True, True]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x >= 0 or x <= 2).collect()
        assert res  == [True, True, True, True, True]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x > 0 or x < 2).collect()
        assert res  == [True, True, True, True, True]



        # combo
        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x > 0 or x > 1 and x < 2).collect()
        assert res  == [False, False, True, True, True]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x > 0 or 1 < x < 2).collect()
        assert res  == [False, False, True, True, True]

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: x < 0 and x < 1 or x > 2).collect()
        assert res  == [True, False, False, False, True]

        del c

    def testReductions(self):
        c = Context(self.conf)

        res = c.parallelize([-1, 0, 1, 2, 3]).map(lambda x: True or False and True).collect()
        assert res == [True] * 5

        res = c.parallelize([1, 2, 3, 4]).map(lambda x: 10 < 20 and 20 > 90).collect()
        assert res == [False] * 4

        del c

    def testReductionsAuto(self):
        c = Context(self.conf)

        # following tests are auto generated using a python script
        res = c.parallelize([20]).map(lambda x: 30.0 == True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True <= 0 == 5).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 == 5).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 < 5 == -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False == -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 0 != True == 30.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 <= 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: True < 10 <= -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 < -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True == -65 <= 5).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True <= False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False == False <= True).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 30.0 < 5).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True != True <= False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -1.0 == -1.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 <= True < 30.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 0 == True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 <= 0 != -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True < 0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 < 30.0 >= 30.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -65 > 30.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 >= -1.0 <= 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -1.0 != -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: True == True < True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 == 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 0 < False > 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False == True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 <= 0 > -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 0 >= -1.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 30.0 > 0 < 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: True <= -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False <= 30.0 != True).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 == 0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 0 != 10 != -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 < False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 > 0 != True).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: False != True).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -65 > True > 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 >= -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -65 < 5 != -1.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -65 >= -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 == 0 != 5).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 > -1.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 30.0 > 0 <= -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -1.0 != 5).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 > True <= -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 >= 0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -1.0 < 5 < -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 <= 0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 < False <= 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 <= -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True == False <= False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False > 0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -1.0 <= -1.0 < 0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -1.0 == True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 == -1.0 <= False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 == True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -1.0 != 0 < False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 == 0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True < False > True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 <= -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 10 != 5 <= 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: False < False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 == True >= 0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False >= True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 < 0 >= -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 >= 0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 0 < 5 >= -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 10 >= 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 30.0 <= True == -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True < 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 <= -1.0 == False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 0 != 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 30.0 == 10 > -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False == 30.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 == -65 < False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False >= False).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -65 >= True >= 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 > 0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -1.0 >= True >= 0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True >= False).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 30.0 != 30.0 > True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 == 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True <= False >= True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 == 30.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 30.0 != 30.0 != 0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 == 5).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: False < 0 != -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 0 <= 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: False != 10 < False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 <= False).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -65 <= 30.0 > 30.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -1.0 >= True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 <= 0 <= -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 > -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 0 > -65 < True).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -1.0 == True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -1.0 == 5 >= 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 >= True).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 < 0 >= 30.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True > 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False <= 30.0 == 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 0 == -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 <= False >= 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 <= 0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 > 10 < True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -1.0 != 30.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 != False <= 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -1.0 < -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 < 10 >= -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: False <= 30.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 == 0 < False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 != 0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: True > False == 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -1.0 != 30.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -65 > 10 >= True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 < -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True == -1.0 >= 30.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 != True).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 < 10 >= False).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: False != False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False < 30.0 >= 0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 >= -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -1.0 != -1.0 == 0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 0 > 5).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 0 != 30.0 <= False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True != -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: False < 10 > 5).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 0 < 5).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: True <= False != 5).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 < 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -65 != 30.0 < 5).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 <= 30.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 0 >= False <= 0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -1.0 != -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -1.0 >= False <= 0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 < 30.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 10 < -65 != False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -1.0 <= -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 >= 30.0 == 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False >= 5).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 <= False < -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 == True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True >= 5 <= 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 < 0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 < 5 < 30.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 0 > -1.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 10 >= 0 <= -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 > -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False == True > -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 <= 5).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: True == 10 >= False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 0 != -1.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: False == -65 == True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 != True).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 < True > 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True < 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: False >= -1.0 <= True).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: True != 30.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 <= 30.0 > 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: False < -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 0 != True < False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 == 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 != 0 > 30.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 == -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 0 != 5 >= -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: False > 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 > 10 < 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 != 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: False > 30.0 <= True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 >= 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -1.0 < -1.0 == -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -1.0 < 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 != True <= 5).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -65 == -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 10 <= False == -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 < -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 == 30.0 != -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 5 > 30.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 10 > 10 <= -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 < -65).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 30.0 == 10 > False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -1.0 == False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: True < 30.0 == 5).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False >= 5).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False >= -1.0 < 30.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -65 != 10).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -1.0 < -65 > 5).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False > True).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: 0 >= True <= -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -1.0 != -1.0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 >= 0 == 10).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 < 30.0).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 > True > False).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: 5 == False).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: -65 < 10 != -65).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -1.0 <= 5).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: -1.0 > True != 0).collect()
        assert res == [False]

        res = c.parallelize([20]).map(lambda x: False < True).collect()
        assert res == [True]

        res = c.parallelize([20]).map(lambda x: False == -1.0 > -1.0).collect()
        assert res == [False]

        del c