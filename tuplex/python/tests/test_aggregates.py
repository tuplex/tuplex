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
import functools
import random
import numpy as np
from tuplex import *

class TestAggregates(unittest.TestCase):
    def setUp(self):
        self.conf = {"webui.enable": False, "driverMemory": "8MB", "partitionSize": "256KB"}

    def test_simple_count(self):
        c = Context(self.conf)

        data = [1, 2, 3, 4, 5, 6]
        res = c.parallelize(data).aggregate(lambda a, b: a + b, lambda a, x: a + 1, 0).collect()
        self.assertEqual(res[0], len(data))

    def test_simple_sum(self):
        c = Context(self.conf)

        data = [1, 2, 3, 4, 5, 6]
        res = c.parallelize(data).aggregate(lambda a, b: a + b, lambda a, x: a + x, 0).collect()
        self.assertEqual(res[0], sum(data))

    def test_sum_by_key(self):
        c = Context(self.conf)

        data = [(0, 10.0), (1, 20.0), (0, -4.5)]

        res = c.parallelize(data, columns=['id', 'volume']).aggregateByKey(lambda a, b: a + b,
                                                                            lambda a, x: a + x['volume'],
                                                                            0.0,
                                                                            ['id']).collect()

        self.assertEqual(len(res), 2)

        # sort result for comparison (in the future Tuplex should provide a function for this!)
        res = sorted(res, key=lambda t: t[0])

        self.assertEqual(res[0][0], 0)
        self.assertEqual(res[1][0], 1)

        self.assertAlmostEqual(res[0][1], 10.0 - 4.5)
        self.assertAlmostEqual(res[1][1], 20.0)
