#!/usr/bin/env python3
#----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 10/16/2021                                                                 #
#  License: Apache 2.0                                                                                                 #
#----------------------------------------------------------------------------------------------------------------------#

import tempfile
import unittest
from tuplex import *

class TestExceptions(unittest.TestCase):

    def setUp(self):
        self.conf = {"webui.enable": False, "driverMemory": "8MB", "partitionSize": "256KB"}
        self.conf_in_order = {"webui.enable": False, "driverMemory": "8MB", "partitionSize": "256KB", "tuplex.optimizer.mergeExceptionsInOrder": True}

    def test_csv(self):
        c = Context(self.conf)

        output = c.csv("resources/type_violation.csv").map(lambda x, y: x).collect()
        print(output)

    def test_no_normal_rows_in_result(self):
        c = Context(self.conf)

        output = c.parallelize([1, None, "a", 1.2, 3, 4]).filter(lambda x: x is None or x == "a" or x == 1.2).collect()
        self.compare([None, "a", 1.2], output)

    def test_empty_result(self):
        c = Context(self.conf)

        output = c.parallelize([1, None, "a", 1.2, 3, 4]).filter(lambda x: x == -1).collect()
        self.compare([], output)

    def test_no_pipeline(self):
        c = Context(self.conf)

        output = c.parallelize([1, 2, 3, 4, "abc"]).collect()
        self.compare([1, 2, 3, 4, "abc"], output)

        output = c.parallelize([1, 2, "abc", 4, 5]).collect()
        self.compare([1, 2, "abc", 4, 5], output)

        output = c.parallelize(["abc", 2, 3, 4, 5]).collect()
        self.compare(["abc", 2, 3, 4, 5], output)

        output = c.parallelize(["abc", 2.4, 4, 5, True]).collect()
        self.compare(["abc", 2.4, 4, 5, True], output)

    def test_single_tuples_unwrapped(self):
        c = Context(self.conf)

        output = c.parallelize([(1,), (2,), (3,)]).collect()
        self.compare([1, 2, 3], output)

    def test_parallelize_exceptions_unwrapped(self):
        c = Context(self.conf)

        output = c.parallelize([1, 2, 3, 4, (None,)]).map(lambda x: x).collect()
        self.compare([1, 2, 3, 4, None], output)

    def test_parallelize_exceptions_no_merge(self):
        c = Context(self.conf)

        output = c.parallelize([1, 2, 3, 4, None]).map(lambda x: x).collect()
        self.compare([1, 2, 3, 4, None], output)

        output = c.parallelize([1, 2, 3, "a", 4]).map(lambda x: x).collect()
        self.compare([1, 2, 3, 4, "a"], output)

        output = c.parallelize([1, 0.3, 2, 3, 4]).map(lambda x: x).collect()
        self.compare([1, 2, 3, 4, 0.3], output)

        output = c.parallelize([(-1, -1), 1, 2, 3, 4]).map(lambda x: x).collect()
        self.compare([1, 2, 3, 4, (-1, -1)], output)

        output = c.parallelize([(True, 1), (True, 2), (True, 3), ("abc", "def")]).map(lambda x: x).collect()
        self.compare([(True, 1), (True, 2), (True, 3), ("abc", "def")], output)

    def compare(self, expectedOutput, output):
        self.assertEqual(len(expectedOutput), len(output))
        for i in range(len(expectedOutput)):
            self.assertEqual(expectedOutput[i], output[i])

    def test_withColumn(self):
        c = Context(self.conf_in_order)

        ds = c.parallelize([(1, "a", True), (0, "b", False), (3, "c", True)])\
            .withColumn("new", lambda x, y, z: str(1 // x) + y)
        output = ds.collect()
        ecounts = ds.exception_counts

        self.assertEqual(2, len(output))
        self.assertEqual((1, "a", True, "1a"), output[0])
        self.assertEqual((3, "c", True, "0c"), output[1])

        self.assertEqual(1, len(ecounts))
        self.assertEqual(1, ecounts["ZeroDivisionError"])

        ds = ds.resolve(ZeroDivisionError, lambda x, y, z: "NULL")
        output = ds.collect()
        ecounts = ds.exception_counts

        self.assertEqual(3, len(output))
        self.assertEqual((1, "a", True, "1a"), output[0])
        self.assertEqual((0, "b", False, "NULL"), output[1])
        self.assertEqual((3, "c", True, "0c"), output[2])

        self.assertEqual(0, len(ecounts))

    def test_mapColumn(self):
        c = Context(self.conf_in_order)

        ds = c.parallelize([(1, "a"), (0, "b"), (3, "c")], columns=["int", "str"]) \
            .mapColumn("int", lambda x: 1 // x)
        output = ds.collect()
        ecounts = ds.exception_counts

        self.assertEqual(2, len(output))
        self.assertEqual((1, "a"), output[0])
        self.assertEqual((0, "c"), output[1])

        self.assertEqual(1, len(ecounts))
        self.assertEqual(1, ecounts["ZeroDivisionError"])

        ds = ds.resolve(ZeroDivisionError, lambda x: -1)
        output = ds.collect()
        ecounts = ds.exception_counts

        self.assertEqual(3, len(output))
        self.assertEqual((1, "a"), output[0])
        self.assertEqual((-1, "b"), output[1])
        self.assertEqual((0, "c"), output[2])

        self.assertEqual(0, len(ecounts))

    def test_withColumn_replace(self):
        c = Context(self.conf_in_order)

        ds = c.parallelize([(1, "a", True), (0, "b", False), (3, "c", True)], columns=["num", "str", "bool"]) \
            .withColumn("str", lambda x, y, z: str(1 // x) + y)
        output = ds.collect()
        ecounts = ds.exception_counts

        self.assertEqual(2, len(output))
        self.assertEqual((1, "1a", True), output[0])
        self.assertEqual((3, "0c", True), output[1])

        self.assertEqual(1, len(ecounts))
        self.assertEqual(1, ecounts["ZeroDivisionError"])

        ds = ds.resolve(ZeroDivisionError, lambda x, y, z: "NULL")
        output = ds.collect()
        ecounts = ds.exception_counts

        self.assertEqual(3, len(output))
        self.assertEqual((1, "1a", True), output[0])
        self.assertEqual((0, "NULL", False), output[1])
        self.assertEqual((3, "0c", True), output[2])

        self.assertEqual(0, len(ecounts))

    def test_map(self):
        c = Context(self.conf_in_order)

        ds = c.parallelize([1, 0, 0, 2]).map(lambda x: 1 // x)
        output = ds.collect()
        ecounts = ds.exception_counts

        self.assertEqual(2, len(output))
        self.assertEqual(1, output[0])
        self.assertEqual(0, output[1])

        self.assertEqual(1, len(ecounts))
        self.assertEqual(2, ecounts["ZeroDivisionError"])

        ds = ds.resolve(ZeroDivisionError, lambda x: -1)
        output = ds.collect()
        ecounts = ds.exception_counts

        self.assertEqual(4, len(output))
        self.assertEqual(1, output[0])
        self.assertEqual(-1, output[1])
        self.assertEqual(-1, output[2])
        self.assertEqual(0, output[3])

        self.assertEqual(0, len(ecounts))

    def test_filter(self):
        c = Context(self.conf_in_order)

        ds = c.parallelize([1, 0, 0, 2]).filter(lambda x: (1 / x) < 5)
        output = ds.collect()
        ecounts = ds.exception_counts

        self.assertEqual(2, len(output))
        self.assertEqual(1, output[0])
        self.assertEqual(2, output[1])

        self.assertEqual(1, len(ecounts))
        self.assertEqual(2, ecounts["ZeroDivisionError"])

        ds = ds.resolve(ZeroDivisionError, lambda x: True)
        output = ds.collect()
        ecounts = ds.exception_counts

        self.assertEqual(4, len(output))
        self.assertEqual(1, output[0])
        self.assertEqual(0, output[1])
        self.assertEqual(0, output[2])
        self.assertEqual(2, output[3])

        self.assertEqual(0, len(ecounts))

