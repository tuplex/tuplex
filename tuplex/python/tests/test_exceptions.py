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

import unittest
import pytest
from tuplex import Context
from random import randint, sample, shuffle
from math import floor
from helper import options_for_pytest


class TestExceptions:

    def setup_method(self, method):
        self.conf = options_for_pytest()
        self.conf.update({"tuplex.webui.enable": False, "executorCount": 8, "executorMemory": "256MB", "driverMemory": "256MB", "partitionSize": "256KB", "tuplex.optimizer.mergeExceptionsInOrder": False})
        self.conf_in_order = options_for_pytest()
        self.conf_in_order.update({"tuplex.webui.enable": False, "executorCount": 8, "executorMemory": "256MB", "driverMemory": "256MB", "partitionSize": "256KB", "tuplex.optimizer.mergeExceptionsInOrder": True})

    def assertEqual(self, lhs, rhs):
        assert lhs == rhs

    def assertTrue(self, ans):
        assert ans

    def test_merge_with_filter(self):
        c = Context(self.conf_in_order)

        output = c.parallelize([0, "e1", 0]).filter(lambda x: x != 0).collect()
        self.compare_in_order(["e1"], output)
        output = c.parallelize([0, 0, "e1"]).filter(lambda x: x != 0).collect()
        self.compare_in_order(["e1"], output)
        output = c.parallelize(["e1", 0, 0]).filter(lambda x: x != 0).collect()
        self.compare_in_order(["e1"], output)

        output = c.parallelize([-1.1, 1, 2, -2.2, 4, 5, -6.6]).filter(lambda x: x < 0 or x > 3).collect()
        self.compare_in_order([-1.1, -2.2, 4, 5, -6.6], output)

    @pytest.mark.parametrize("n", [1000, 2500])
    def test_merge_with_filter(self, n):
        c = Context(self.conf_in_order)
        input = list(range(1, n + 1))
        sampled = sample(input, int(0.4 * n))
        for i in sampled:
            ind = randint(0, 1)
            if ind == 0:
                input[i - 1] = str(input[i - 1])
            elif ind == 1:
                input[i - 1] = 0

        output = c.parallelize(input).filter(lambda x: x != 0).collect()
        self.compare_in_order(list(filter(lambda x: x != 0, input)), output)

    def process(self, input_size, num_filtered, num_schema, num_resolved, num_unresolved):
        inds = list(range(input_size))
        shuffle(inds)
        inds = iter(inds)

        input = list(range(1, input_size + 1))

        for _ in range(floor(num_filtered * input_size)):
            ind = next(inds)
            input[ind] = -1

        for _ in range(floor(num_schema * input_size)):
            ind = next(inds)
            input[ind] = "E"

        for _ in range(floor(num_resolved * input_size)):
            ind = next(inds)
            input[ind] = -2

        for _ in range(floor(num_unresolved * input_size)):
            ind = next(inds)
            input[ind] = -3

        def filter_udf(x):
            return x != -1

        def map_udf(x):
            if x == -2 or x == -3:
                return 1 // (x - x)
            else:
                return x

        def resolve_udf(x):
            if x == -3:
                return 1 // (x - x)
            else:
                return x

        # for larger partitions, there's a multi-threading issue for this.
        # need to fix.
        conf = self.conf_in_order

        # use this line to force single-threaded
        conf['executorCount'] = 0

        c = Context(conf)
        output = c.parallelize(input).filter(filter_udf).map(map_udf).resolve(ZeroDivisionError, resolve_udf).collect()

        self.assertEqual(list(filter(lambda x: x != -3 and x != -1, input)), output)

    # test tends to be slow on Github actions, do not test for 100k
    @pytest.mark.parametrize("n", [100, 1000, 10000])
    def test_everything(self, n):
        self.process(n, 0.25, 0.25, 0.25, 0.25)

    def test_merge_with_filter_on_exps(self):
        c = Context(self.conf_in_order)

        output = c.parallelize([0, 1.1, 2.2, 1, 3.3, 4, 5]).filter(lambda x: x != 0 and x != 1.1).collect()
        self.compare_in_order([2.2, 1, 3.3, 4, 5], output)

    @pytest.mark.parametrize("n", [10000])
    def test_merge_runtime_only(self, n):
        c = Context(self.conf_in_order)

        output = c.parallelize([1, 0, 0, 4]).map(lambda x: 1 // x).resolve(ZeroDivisionError, lambda x: -1).collect()
        self.compare_in_order([1, -1, -1, 0], output)

        output = c.parallelize([0 for i in range(n)]).map(lambda x: 1 // x).resolve(ZeroDivisionError, lambda x: -1).collect()
        self.compare_in_order([-1 for i in range(n)], output)

        input = []
        for i in range(n):
            if i % 100 == 0:
                input.append(0)
            else:
                input.append(i)

        output = c.parallelize(input).map(lambda x: 1 // x).resolve(ZeroDivisionError, lambda x: -1).collect()

        expectedOutput = []
        for i in range(n):
            if i % 100 == 0:
                expectedOutput.append(-1)
            else:
                expectedOutput.append(1 // i)

        self.compare_in_order(expectedOutput, output)

    def test_merge_some_fail(self):
        c = Context(self.conf_in_order)

        input = [1, 2, -1, 5, 6, 7, -2, 10, 11, 12, -3, 15]
        output = c.parallelize(input) \
            .map(lambda x: 1 // (x - x) if x == -1 or x == -2 or x == -3 else x) \
            .resolve(ZeroDivisionError, lambda x: 1 // (x - x) if x == -2 else x) \
            .collect()
        self.compare_in_order([1, 2, -1, 5, 6, 7, 10, 11, 12, -3, 15], output)

    @pytest.mark.parametrize("n", [10000])
    def test_merge_both_but_no_resolve(self, n):
        c = Context(self.conf_in_order)

        input = [1, 2, -1, "a", 5, 6, 7, -2, "b", 10, 11, 12, -3, "c", 15]
        output = c.parallelize(input) \
            .map(lambda x: 1 // (x - x) if x == -1 or x == -2 or x == -3 else x) \
            .resolve(ZeroDivisionError, lambda x: 1 // (x - x) if x == -2 else x) \
            .collect()
        self.compare_in_order([1, 2, -1, "a", 5, 6, 7, "b", 10, 11, 12, -3, "c", 15], output)

        input = list(range(1, n + 1))
        sampled = sample(input, int(0.4 * n))
        for i in sampled:
            ind = randint(0, 2)
            if ind == 0:
                input[i - 1] = str(input[i - 1])
            elif ind == 1:
                input[i - 1] = 0
            else:
                input[i - 1] = -1
        expectedOutput = list(filter(lambda x: x != 0, input))

        output = c.parallelize(input).map(lambda x: 1 // (x - x) if x == -1 or x == 0 else x).resolve(ZeroDivisionError, lambda x: 1 // x if x == 0 else x).collect()
        self.compare_in_order(expectedOutput, output)

    @pytest.mark.parametrize("n", [10000])
    def test_merge_both(self, n):
        c = Context(self.conf_in_order)

        input = [1, 2, 0, "a", 5, 6, 7, 0, "b", 10, 11, 12, 0, "c", 15]
        output = c.parallelize(input).map(lambda x: 1 // x if x == 0 else x).resolve(ZeroDivisionError, lambda x: -1).collect()
        self.compare_in_order([1, 2, -1, "a", 5, 6, 7, -1, "b", 10, 11, 12, -1, "c", 15], output)

        input = [1, 2, "a", 0, 5, 6, 7, "b", 0, 10, 11, 12, "c", 0, 15]
        output = c.parallelize(input).map(lambda x: 1 // x if x == 0 else x).resolve(ZeroDivisionError, lambda x: -1).collect()
        self.compare_in_order([1, 2, "a", -1, 5, 6, 7, "b", -1, 10, 11, 12, "c", -1, 15], output)

        input = list(range(1, n + 1))
        sampled = sample(input, int(0.4 * n))
        for i in sampled:
            if randint(0, 1) == 0:
                input[i - 1] = str(input[i - 1])
            else:
                input[i - 1] = 0

        output = c.parallelize(input).map(lambda x: 1 // x if x == 0 else x).resolve(ZeroDivisionError, lambda x: x).collect()
        self.compare_in_order(input, output)

    # 40k too slow under macOS, need to investigate
    @pytest.mark.parametrize("n", [10000])
    def test_merge_input_only(self, n):
        c = Context(self.conf_in_order)

        input = [1, 2, "a", 4, 5, "b", 6, 7, 8, 9, 10, "d"]
        output = c.parallelize([1, 2, "a", 4, 5, "b", 6, 7, 8, 9, 10, "d"]).map(lambda x: x).collect()
        self.compare_in_order(input, output)

        input = []
        for i in range(n):
            if i % 100 == 0:
                input.append(str(i))
            else:
                input.append(i)

        output = c.parallelize(input).map(lambda x: x).collect()
        self.compare_in_order(input, output)

    def test_no_normal_rows_in_result(self):
        c = Context(self.conf)

        output = c.parallelize([1, None, "a", 1.2, 3, 4]).filter(lambda x: x != 1 and x != 3 and x != 4).collect()
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

    def test_no_merge_some_fail(self):
        c = Context(self.conf)

        input = [1, 2, -1, 5, 6, 7, -2, 10, 11, 12, -3, 15]
        output = c.parallelize(input) \
            .map(lambda x: 1 // (x - x) if x == -1 or x == -2 or x == -3 else x) \
            .resolve(ZeroDivisionError, lambda x: 1 // (x - x) if x == -2 else x) \
            .collect()
        self.compare([1, 2, -1, 5, 6, 7, 10, 11, 12, -3, 15], output)

    @pytest.mark.parametrize("n", [10000])
    def test_no_merge_both_but_no_resolve(self, n):
        c = Context(self.conf)

        input = [1, 2, -1, "a", 5, 6, 7, -2, "b", 10, 11, 12, -3, "c", 15]
        output = c.parallelize(input) \
            .map(lambda x: 1 // (x - x) if x == -1 or x == -2 or x == -3 else x) \
            .resolve(ZeroDivisionError, lambda x: 1 // (x - x) if x == -2 else x) \
            .collect()
        self.compare([1, 2, -1, "a", 5, 6, 7, "b", 10, 11, 12, -3, "c", 15], output)

        input = list(range(1, n + 1))
        sampled = sample(input, int(0.4 * n))
        for i in sampled:
            ind = randint(0, 2)
            if ind == 0:
                input[i - 1] = str(input[i - 1])
            elif ind == 1:
                input[i - 1] = 0
            else:
                input[i - 1] = -1
        expectedOutput = list(filter(lambda x: x != 0, input))

        output = c.parallelize(input).map(lambda x: 1 // (x - x) if x == -1 or x == 0 else x).resolve(ZeroDivisionError, lambda x: 1 // x if x == 0 else x).collect()
        self.compare(expectedOutput, output)

    @pytest.mark.parametrize("n", [10000])
    def test_no_merge_both(self, n):
        c = Context(self.conf)

        input = [1, 2, 0, "a", 5, 6, 7, 0, "b", 10, 11, 12, 0, "c", 15]
        output = c.parallelize(input).map(lambda x: 1 // x if x == 0 else x).resolve(ZeroDivisionError, lambda x: -1).collect()
        self.compare([1, 2, -1, "a", 5, 6, 7, -1, "b", 10, 11, 12, -1, "c", 15], output)

        input = [1, 2, "a", 0, 5, 6, 7, "b", 0, 10, 11, 12, "c", 0, 15]
        output = c.parallelize(input).map(lambda x: 1 // x if x == 0 else x).resolve(ZeroDivisionError, lambda x: -1).collect()
        self.compare([1, 2, "a", -1, 5, 6, 7, "b", -1, 10, 11, 12, "c", -1, 15], output)

        input = list(range(1, n + 1))
        sampled = sample(input, int(0.4 * n))
        for i in sampled:
            if randint(0, 1) == 0:
                input[i - 1] = str(input[i - 1])
            else:
                input[i - 1] = 0

        output = c.parallelize(input).map(lambda x: 1 // x if x == 0 else x).resolve(ZeroDivisionError, lambda x: x).collect()
        self.compare(input, output)

    # 40k too slow under macOS, need to investigate.
    @pytest.mark.parametrize("n", [10000])
    def test_no_merge_input_only(self, n):
        c = Context(self.conf)

        input = [1, 2, "a", 4, 5, "b", 6, 7, 8, 9, 10, "d"]
        output = c.parallelize([1, 2, "a", 4, 5, "b", 6, 7, 8, 9, 10, "d"]).map(lambda x: x).collect()
        self.compare(input, output)

        input = []
        for i in range(n):
            if i % 100 == 0:
                input.append(str(i))
            else:
                input.append(i)

        output = c.parallelize(input).map(lambda x: x).collect()
        self.compare(input, output)

    @pytest.mark.parametrize("n", [10000])
    def test_no_merge_runtime_only(self, n):
        c = Context(self.conf)

        output = c.parallelize([1, 0, 0, 4]).map(lambda x: 1 // x).resolve(ZeroDivisionError, lambda x: -1).collect()
        self.compare([1, -1, -1, 0], output)

        input = []
        for i in range(n):
            if i % 100 == 0:
                input.append(0)
            else:
                input.append(i)

        output = c.parallelize(input).map(lambda x: 1 // x).resolve(ZeroDivisionError, lambda x: -1).collect()

        expectedOutput = []
        for i in range(n):
            if i % 100 == 0:
                expectedOutput.append(-1)
            else:
                expectedOutput.append(1 // i)

        self.compare(expectedOutput, output)

    # 50k too slow under macOS, need to investigate
    @pytest.mark.parametrize("n", [10000])
    def test_parallelize_exceptions_no_merge(self, n):
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

        l1 = []
        l2 = []
        input = []
        for i in range(n):
            if i % 100 == 0:
                l2.append(str(i))
                input.append(str(i))
            else:
                l1.append(i)
                input.append(i)
        output = c.parallelize(input).map(lambda x: x).collect()
        l1.extend(l2)
        self.compare(l1, output)

    def compare(self, expectedOutput, output):
        self.assertEqual(len(expectedOutput), len(output))
        expectedOutput = set(expectedOutput)
        output = set(output)
        for elt in expectedOutput:
            self.assertTrue(elt in output)

    def compare_in_order(self, expectedOutput, output):
        self.assertEqual(len(expectedOutput), len(output))
        for i in range(len(expectedOutput)):
            self.assertEqual(expectedOutput[i], output[i])

    def test_withColumn(self):
        c = Context(self.conf_in_order)

        ds = c.parallelize([(1, "a", True), (0, "b", False), (3, "c", True)]) \
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

        ds = c.parallelize([1, 0, 0, 2]).filter(lambda x: (1 // x) < 5)
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

