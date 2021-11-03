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
import typing
import unittest
from tuplex import *

class TestExceptions(unittest.TestCase):

    def setUp(self):
        self.conf = {"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB", "tuplex.optimizer.mergeExceptionsInOrder": True}

    def test_mixedIsNone(self):
        c = Context(self.conf)
        res = c.parallelize([(1, None), (2, 255), (3, 400), (4, False), (5, 2.3)]).map(lambda x, y: (y, y == None)).collect()
        self.assertEqual([(None, True), (255, False), (400, False), (False, False), (2.3, False)], res)

    def test_mixedIsNone2(self):
        c = Context(self.conf)
        res = c.parallelize([None, 255, 400, False, 2.3]).map(lambda x: (x, x == None)).collect()
        print(res)
        # self.assertEqual([(None, True), (255, False), (400, False), (False, False), (2.3, False)], res)

    def test_examples(self):
        c = Context()

        # Exceptions occur in map UDF
        ds = c.parallelize(...).map(...)
        ds = ds.resolve(...)

        # Exceptions occur in filter UDF
        ds = c.parallelize(...).filter(...)
        ds = ds.resolve(...)

        # Exceptions occur in csv reading
        ds = c.csv(...)
        ds = c.csv

    def test_join(self):
        c = Context(self.conf)

        def f1(x):
            if x == "":
                raise ValueError
            return x.lower()

        ds1 = c.parallelize([(0, "Ben"), (1, "Hannah"), (2, "Sam"), (3, "")], ["id", "name"])\
            .mapColumn("name", f1)
        ds2 = c.parallelize([(0, 21), (1, 25), (2, 27)], ["id", "age"])\
            .join(ds1, "id", "id") \
            .mapColumn("id", lambda x: 1 / x)
        print(ds2.collect())
        print(ds1.exception_counts)
        print(ds1.exceptions)
        print(ds2.exception_counts)
        print(ds2.exceptions)

    def test_withColumn(self):
        c = Context(self.conf)

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
        c = Context(self.conf)

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
        c = Context(self.conf)

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
        c = Context(self.conf)

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
        c = Context(self.conf)

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

    def test_example(self):
        c = Context(self.conf)

        ds = c.csv("resources/test.csv").map(lambda x, y: x + y)

        output = ds.collect()
        ecounts = ds.exception_counts

        self.assertEqual(5, len(output))
        self.assertEqual(3, output[0])
        self.assertEqual(7, output[1])
        self.assertEqual(11, output[2])
        self.assertEqual(15, output[3])
        self.assertEqual(19, output[4])

        self.assertEqual(1, len(ecounts))
        self.assertEqual(2, ecounts["TypeError"])

        ds = ds.resolve(TypeError, lambda x, y: (x if x else 0) + (y if y else 0))
        output = ds.collect()
        ecounts = ds.exception_counts

        self.assertEqual(7, len(output))
        self.assertEqual(3, output[0])
        self.assertEqual(7, output[1])
        self.assertEqual(11, output[2])
        self.assertEqual(15, output[3])
        self.assertEqual(19, output[4])
        self.assertEqual(100, output[5])
        self.assertEqual(200, output[6])

        self.assertEqual(0, len(ecounts))

    def test_example(self):
        c = Context(self.conf)

        ds = c.csv("resources/test3.csv").map(lambda x, y: (x, len(y)))
        print(ds.collect())

    def test_example2(self):
        c = Context(self.conf)

        # Tuplex saves 2 exception rows with BadParseStringInput
        ds = c.csv("resources/test2.csv", type_hints={0: int, 1: int})
        print(ds.collect())
        print(ds.exception_counts)

        # ds = c.csv("resources/test2.csv", type_hints={0: int, 1: typing.Union[int, str]})
        # print(ds.collect())

    def test_example3(self):
        c = Context(self.conf)

        # Tuplex resolves 2 exception rows and types data as optional[Int]
        ds = c.csv("resources/test2.csv")
        print(ds.collect())
        print(ds.exception_counts)

    def test_example4(self):
        c = Context(self.conf)

        # Tuplex ignores exception rows entirely
        ds = c.parallelize([(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 7), (7, 7), (8, 8), (9, 9), (10, "b"), (11, "a")]).map(lambda x: x[0])
        print(ds.collect())
        print(ds.exception_counts)

    def test_example5(self):
        c = Context(self.conf)

        # Tuplex ignores exception rows entirely
        ds = c.parallelize([(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 7), (7, 7), (8, 8), (9, 9), 10, (10, "a")], schema=(int, int))
        print(ds.collect())
        print(ds.exception_counts)

    def test_examaple1(self):
        c = Context()

        ds = c.csv("resources/test.csv", type_hints={0: int, 1: int})

        output = ds.collect()
        ecounts = ds.exception_counts
        exceptions = ds.exceptions

        print(output)
        print(ecounts)
        print(exceptions)

        c.csv("resources/test.csv", type_hints={0: int, 1: int})

        #
        # ds = ds.resolve(Exception, lambda x: x + 1)
        #
        # output = ds.collect()
        # ecounts = ds.exception_counts
        #
        # print(output)
        # print(ecounts)

    def test_flights(self):
        conf = {
            "tuplex.optimizer.mergeExceptionsInOrder": True,
            "webui.enable": False,
            "executorCount" : 8,
            "executorMemory" : "2G",
            "driverMemory" : "2G",
            "partitionSize" : "32MB",
            "runTimeMemory" : "128MB",
        }

        c = Context(conf)

        ds = c.csv("../../test/resources/pipelines/flights/flights_on_time_performance_2009_01.sample.csv")

        ds.cache()
        print(ds.exception_counts)
