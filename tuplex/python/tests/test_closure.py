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

from unittest import TestCase
import tuplex
import time
from helper import test_options

class TestClosure(TestCase):

    def setUp(self):
        self.conf = test_options()
        self.c = tuplex.Context(self.conf)


    def testGlobalVar(self):

        # function capturing global g
        g = 20
        def f(x):
            return x + g

        res = self.c.parallelize([1, 2, 3]).map(f).collect()
        self.assertEqual(res, [21, 22, 23])

        res = self.c.parallelize([1, 2, 3]).map(lambda x: x * g).collect()
        self.assertEqual(res, [20, 40, 60])