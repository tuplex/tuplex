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
from .helper import test_options

class TestArithmetic(unittest.TestCase):
    def setUp(self):
        self.conf = test_options()
        self.conf.update({"webui.enable": False, "driverMemory": "8MB",
                          "partitionSize": "256KB", "tuplex.optimizer.mergeExceptionsInOrder": True})

    def test_add(self):
        c = Context(self.conf)
        res = c.parallelize([1, 2, 4]).map(lambda x: x + 10.7).collect()
        assert res == [11.7, 12.7, 14.7]

        res = c.parallelize([1, 2, 4]).map(lambda x: +x).collect()
        assert res == [1, 2, 4]

        res = c.parallelize([True, False]).map(lambda x: +x).collect()
        assert res == [1, 0]

        res = c.parallelize([1.1, 2.1, 4.1]).map(lambda x: +x).collect()
        assert res == [1.1, 2.1, 4.1]

    def test_sub(self):
        c = Context(self.conf)
        res = c.parallelize([11, 12, 13]).map(lambda x: x - 10).collect()
        assert res == [1, 2, 3]

        res = c.parallelize([11, 12, 13]).map(lambda x: -x).collect()
        assert res == [-11, -12, -13]

        res = c.parallelize([0]).map(lambda x: x - True).collect()
        assert res == [-1]

        res = c.parallelize([0]).map(lambda x: -True).collect()
        assert res == [-1]

        res = c.parallelize([0]).map(lambda x: -False).collect()
        assert res == [0]

        res = c.parallelize([0]).map(lambda x: True - False).collect()
        assert res == [1]

        res = c.parallelize([0]).map(lambda x: True - 0.1).collect()
        assert res == [0.9]

        res = c.parallelize([1, 2, 3]).map(lambda x: x - 0.1).collect()
        assert res == [0.9, 1.9, 2.9]

        res = c.parallelize([1.1, 2.1, 3.1]).map(lambda x: x - 1).collect()
        assert all(map(lambda x: abs(x[0] - x[1]) < 1e-6, zip(res, [0.1, 1.1, 2.1])))

    def test_bitwise_negation(self):
        c = Context(self.conf)
        res = c.parallelize([0, 1, 2]).map(lambda x: ~x).collect()
        assert res == [-1, -2, -3]

        res = c.parallelize([True, False]).map(lambda x: ~x).collect()
        assert res == [-2, -1]

    def test_div(self):
        c = Context(self.conf)
        res = c.parallelize([0, 1, 2, -5, -10]).map(lambda x: x / 10).collect()
        assert res == [0, 0.1, 0.2, -0.5, -1.0]

        # exceptions
        res = c.parallelize([(0, 0), (-1, 0), (1, 0), (42, 1)]).map(lambda x: x[0] / x[1]).collect()
        assert res == [42]

    def test_idiv(self):
        c = Context(self.conf)
        res = c.parallelize([10, 11, 12, 13, 14, 15, 16]).map(lambda x: x // 7).collect()
        assert res == [1, 1, 1, 1, 2, 2, 2]

        res = c.parallelize([10, 11, 12, 13, 14, 15, 16]).map(lambda x: x // 7.0).collect()
        assert res == [1.0, 1.0, 1.0, 1.0, 2.0, 2.0, 2.0]

        res = c.parallelize([-10, -9, -8, -7, -6, -5]).map(lambda x: x // 6).collect()
        assert res == [-2, -2, -2, -2, -1, -1]

        res = c.parallelize([-10, -9, -8, -7, -6, -5]).map(lambda x: x // -6).collect()
        assert res == [1, 1, 1, 1, 1, 0]

        res = c.parallelize([-10, -9, -8, -7, -6, -5]).map(lambda x: x // -6.0).collect()
        assert res == [1.0, 1.0, 1.0, 1.0, 1.0, 0.0]

    def test_power(self):
        c = Context(self.conf)
        res = c.parallelize([()])

        # todo... add here test cases ...

    def test_modulo(self):

        c = Context(self.conf)

        # auto construct a bunch of test cases with different types etc.
        xvals = [-10, -3, 0, 5, 7]
        yvals = [-7, 2, 3, 10]

        # i64 % i64
        pairs = []
        expected_res = []
        for x in xvals:
            for y in yvals:
                pairs.append((x, y))
                expected_res.append(x % y)
        res = c.parallelize(pairs).map(lambda a, b: a % b).collect()
        assert res == expected_res

        # i64 % f64
        pairs = []
        expected_res = []
        for x in xvals:
            for y in yvals:
                pairs.append((x, float(y)))
                expected_res.append(x % float(y))
        res = c.parallelize(pairs).map(lambda a, b: a % b).collect()
        assert res == expected_res

        # f64 % i64
        pairs = []
        expected_res = []
        for x in xvals:
            for y in yvals:
                pairs.append((float(x), y))
                expected_res.append(float(x) % y)
        res = c.parallelize(pairs).map(lambda a, b: a % b).collect()
        assert res == expected_res

        # f64 % f64
        pairs = []
        expected_res = []
        for x in xvals:
            for y in yvals:
                pairs.append((float(x), float(y)))
                expected_res.append(float(x) % float(y))
        res = c.parallelize(pairs).map(lambda a, b: a % b).collect()
        assert res == expected_res

    def test_leftshift(self):
        c = Context(self.conf)

        # auto construct a bunch of test cases with different types etc.
        ivals1 = [-10, -3, 0, 5, 7]
        ivals2 = [0, 1, 2, 3, 4]
        bvals = [True, False]

        # i64 << i64
        pairs = []
        expected_res = []
        for x in ivals1:
            for y in ivals2:
                pairs.append((x, y))
                expected_res.append(x << y)
        res = c.parallelize(pairs).map(lambda a, b: a << b).collect()
        assert res == expected_res

        # i64 << bool
        pairs = []
        expected_res = []
        for x in ivals1:
            for y in bvals:
                pairs.append((x, y))
                expected_res.append(x << y)
        res = c.parallelize(pairs).map(lambda a, b: a << b).collect()
        assert res == expected_res

        # bool << i64
        pairs = []
        expected_res = []
        for x in bvals:
            for y in ivals2:
                pairs.append((x, y))
                expected_res.append(x << y)
        res = c.parallelize(pairs).map(lambda a, b: a << b).collect()
        assert res == expected_res

        # bool << bool
        pairs = []
        expected_res = []
        for x in bvals:
            for y in bvals:
                pairs.append((x, y))
                expected_res.append(x << y)
        res = c.parallelize(pairs).map(lambda a, b: a << b).collect()
        assert res == expected_res

    def test_rightshift(self):
        c = Context(self.conf)

        # auto construct a bunch of test cases with different types etc.
        ivals1 = [-10, -3, 0, 5, 7]
        ivals2 = [0, 1, 2, 3, 4]
        bvals = [True, False]

        # i64 >> i64
        pairs = []
        expected_res = []
        for x in ivals1:
            for y in ivals2:
                pairs.append((x, y))
                expected_res.append(x >> y)
        res = c.parallelize(pairs).map(lambda a, b: a >> b).collect()
        assert res == expected_res

        # i64 >> bool
        pairs = []
        expected_res = []
        for x in ivals1:
            for y in bvals:
                pairs.append((x, y))
                expected_res.append(x >> y)
        res = c.parallelize(pairs).map(lambda a, b: a >> b).collect()
        assert res == expected_res

        # bool >> i64
        pairs = []
        expected_res = []
        for x in bvals:
            for y in ivals2:
                pairs.append((x, y))
                expected_res.append(x >> y)
        res = c.parallelize(pairs).map(lambda a, b: a >> b).collect()
        assert res == expected_res

        # bool >> bool
        pairs = []
        expected_res = []
        for x in bvals:
            for y in bvals:
                pairs.append((x, y))
                expected_res.append(x >> y)
        res = c.parallelize(pairs).map(lambda a, b: a >> b).collect()
        assert res == expected_res

    def test_floatcast(self):
        from math import isclose, isnan
        c = Context(self.conf)

        testsets = [
            [-10, 0, 20],
            [-10.123, 0.0, 1.23, float('inf'), float('nan')],
            ["-10", "-3.45", "0", "5", "7.123", "inf", "nan", "InfINIty", "NAn"],
            [True, False]
        ]

        for testset in testsets:
            res = c.parallelize(testset).map(lambda x: float(x)).collect()
            expected_res = list(map(lambda x: float(x), testset))
            assert len(res) == len(expected_res), "value error occurred res: {}".format(res)
            for f1, f2 in zip(res, expected_res):
                assert (isclose(f1, f2) or (isnan(f1) and isnan(f2)))

    def test_boolcast(self):
        c = Context(self.conf)

        testsets = [
            [-10, 0, 20],
            [-10.123, 0.0, 1.23, float('nan'), float('inf')],
            ["-10", "hello", "", "   bye   ", "7.123"],
            [True, False]
        ]

        for testset in testsets:
            res = c.parallelize(testset).map(lambda x: bool(x)).collect()
            expected_res = list(map(lambda x: bool(x), testset))
            assert res == expected_res

    def test_builtin_power(self):
        c = Context(self.conf)

        N = 10

        # test over a couple fixed exponents
        def f(x, p):
            try:
                return x ** p
            except ZeroDivisionError:
                return 42

        data = [0] + [random.randint(-9, 9) for _ in range(N)]
        print(data)
        # int data, int exp
        for exp in range(-5, 6):
            ref = list(map(functools.partial(f, p=exp), data))

            # compute via tuplex
            ds = c.parallelize(data).map(lambda x: x ** exp)
            res = ds.resolve(ZeroDivisionError, lambda x: 42).collect()
            self.assertEqual(len(res), len(ref))
            for i in range(len(res)):
                self.assertAlmostEqual(res[i], ref[i])

        # int data, float exp
        for exp in np.arange(-5.0, 6.0, 0.5):
            exp = float(exp)  # numpy datatype not support yet, hence explicit cast to python float type.
            ref = list(map(functools.partial(f, p=exp), data))

            # compute via tuplex
            ds = c.parallelize(data).map(lambda x: x ** exp)
            res = ds.resolve(ZeroDivisionError, lambda x: 42).collect()
            self.assertEqual(len(res), len(ref))
            for i in range(len(res)):
                self.assertAlmostEqual(res[i], ref[i])

        # float data, int exp
        data = [el * 1.0 for el in data]  # convert to float. Note that numpy arrays are NOT yet supported...
        for exp in range(-5, 6):
            ref = list(map(functools.partial(f, p=exp), data))

            # compute via tuplex
            ds = c.parallelize(data).map(lambda x: x ** exp)
            res = ds.resolve(ZeroDivisionError, lambda x: 42).collect()
            self.assertEqual(len(res), len(ref))
            for i in range(len(res)):
                self.assertAlmostEqual(res[i], ref[i])

        # float data, float exp
        for exp in np.arange(-5.0, 6.0, 0.5):
            exp = float(exp)  # numpy datatype not support yet, hence explicit cast to python float type.
            ref = list(map(functools.partial(f, p=exp), data))

            # compute via tuplex
            ds = c.parallelize(data).map(lambda x: x ** exp)
            res = ds.resolve(ZeroDivisionError, lambda x: 42).collect()
            self.assertEqual(len(res), len(ref))
            for i in range(len(res)):
                self.assertAlmostEqual(res[i], ref[i])