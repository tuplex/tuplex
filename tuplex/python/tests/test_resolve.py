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

class TestResolveMechanism(TestCase):

    def setUp(self):
        self.conf = {"webui.enable": False, "driverMemory": "8MB",
                     "partitionSize": "256KB", "optimizer.mergeExceptionsInOrder": True}
        self.c = tuplex.Context(self.conf)

    def test_LambdaResolveI(self):

        ds = self.c.parallelize([0, 1, 2, 3, 4]).map(lambda x: 1. / x)

        self.assertEqual(ds.collect(), [1. / 1, 1. / 2, 1. / 3, 1. / 4])

        self.assertEqual(ds.resolve(ZeroDivisionError, lambda x: 42).collect(),
                         [42, 1. / 1, 1. / 2, 1. / 3, 1. / 4])

    def test_LambdaResolveII(self):
        ds = self.c.parallelize([(1, 0), (2, 1), (3, 0), (4, 1)]).map(lambda x: x[0] / x[1])

        self.assertEqual(ds.collect(), [2.0, 4.0])

        self.assertEqual(ds.resolve(ZeroDivisionError, lambda x: 42).collect(),
                         [42, 2.0, 42, 4.0])

    def test_counts(self):

        def f(x):
            if x % 2 == 0:
                raise FileNotFoundError
            if x % 3 == 0:
                raise LookupError
            if x % 5 == 0:
                raise IndexError
            return x * x

        ds = self.c.parallelize([1, 2, 3, 4, 5]).map(f)

        self.assertEqual(ds.collect(), [1])

        d = ds.exception_counts

        self.assertEqual(d['FileNotFoundError'], 2)
        self.assertEqual(d['LookupError'], 1)
        self.assertEqual(d['IndexError'], 1)