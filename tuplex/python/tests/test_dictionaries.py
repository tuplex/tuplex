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
from math import isclose
from helper import test_options

class TestDictionaries(unittest.TestCase):

    def setUp(self):
        self.conf = test_options()
        self.conf.update({"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"})

    # test pop(), popitem()
    def test_attributes(self):
        c = Context(self.conf)

        # manual tests
        strings = [('hello', 'world'), ('foo', 'bar'), ('blank', ''), ('', 'another'), ('', '')]
        res = c.parallelize(strings).map(lambda a, b: {1: a, 2: b}.popitem()).collect()
        assert res == [(1, 'hello'), (1, 'foo'), (1, 'blank'), (1, ''), (1,'')]
        res = c.parallelize(strings).map(lambda a, b: {True: a, False: b}.pop(False)).collect()
        assert res == ['world', 'bar', '', 'another', '']

        ints = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        res = c.parallelize(ints).map(lambda a, b, c: {a*1.2: a+c, -4.569: b}.popitem()).collect()
        assert res == [(1.2, 4), (4.8, 10), (8.4, 16)]
        res = c.parallelize(ints).map(lambda a, b, c: {a: True, b: False, c: True}.pop(b)).collect()
        assert res == [False, False, False]

        floats = [(1.2, 3.4), (5.6, 7.8), (9.0, 0.1)]
        res = c.parallelize(floats).map(lambda a, b: {str(a): a+b, str(b): b}.popitem()).collect()
        assert len(res) == len(floats)
        expected_res = [('1.2', 4.6), ('5.6', 13.4), ('9.0', 9.1)]
        for (s1, d1), (s2, d2) in zip(res, expected_res):
            assert s1 == s2
            assert isclose(d1, d2)

        res = c.parallelize(floats).map(lambda a, b: ({1.23: a, -2.3: b},)).map(lambda x: x[0].pop(1.23)).collect()
        assert res == [1.2, 5.6, 9.0]

        # lot of type stress testing
        strings = [('hello', 'world', 'hi'), ('foo', 'bar', 'baz'), ('blank', '', 'not')]
        ints = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        floats = [(1.2, 3.4, -100.2), (5.6, 7.8, -1.234), (9.0, 0.1, 2.3)]
        bools = [(True, False, False), (False, True, True), (False, True, False)]
        for l1 in [strings, ints, floats, bools]:
            for l2 in [strings, ints, floats, bools]:
                l3 = [x + y for x, y in zip(l1, l2)]
                lam1 = lambda x, y, z, a, b, c: {x: a, y: b, z: c}.pop(y)
                lam2 = lambda x: {x[0]: x[3], x[1]: x[4], x[2]: x[5]}.pop(x[1])
                if l1 == bools:  # only two keys
                    l3 = [x[:2] + y[:2] for x, y in zip(l1, l2)]
                    lam1 = lambda x, y, a, b: {x: a, y: b}.pop(y)
                    lam2 = lambda x: {x[0]: x[2], x[1]: x[3]}.pop(x[1])

                res = c.parallelize(l3).map(lam1).collect()
                assert res == list(map(lam2, l3))

    def test_subscripts(self):
        c = Context(self.conf)

        # lot of type stress testing
        strings = [('hello', 'world', 'hi'), ('foo', 'bar', 'baz'), ('blank', '', 'not')]
        ints = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        floats = [(1.2, 3.4, -100.2), (5.6, 7.8, -1.234), (9.0, 0.1, 2.3)]
        bools = [(True, False, False), (False, True, True), (False, True, False)]
        for l1 in [strings, ints, floats, bools]:
            for l2 in [strings, ints, floats, bools]:
                l3 = [x + y for x, y in zip(l1, l2)]
                lam1 = lambda x, y, z, a, b, c: {x: a, y: b, z: c}.pop(y)
                lam2 = lambda x: {x[0]: x[3], x[1]: x[4], x[2]: x[5]}.pop(x[1])
                if l1 == bools:  # only two keys
                    l3 = [x[:2] + y[:2] for x, y in zip(l1, l2)]
                    lam1 = lambda x, y, a, b: {x: a, y: b}.pop(y)
                    lam2 = lambda x: {x[0]: x[2], x[1]: x[3]}.pop(x[1])

                res = c.parallelize(l3).map(lam1).collect()
                assert res == list(map(lam2, l3))

    def test_operators(self):
        c = Context(self.conf)

        strings = [('hello', 'world'), ('foo', 'bar'), ('blank', ''), ('', 'another'), ('', '')]

        res = c.parallelize(strings).map(lambda x: {'col1': x[0], 'col2': x[1]}).mapColumn('col1', lambda x: len(x)).collect()
        assert res == [(5, 'world'), (3, 'bar'), (5, ''), (0, 'another'), (0, '')]

        res = c.parallelize(strings)\
            .map(lambda x: {'col1': x[0], 'col2': x[1]})\
            .mapColumn('col1', lambda x: len(x))\
            .withColumn('col1', lambda x: x['col1'] + len(x['col2']))\
            .collect()
        assert res == [(10, 'world'), (6, 'bar'), (5, ''), (7, 'another'), (0, '')]

        ints = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        res = c.parallelize(ints) \
            .map(lambda x: {'col1': x[0], 'col2': x[1], 'col3': x[2]}) \
            .withColumn('col2', lambda x: x['col2'] - x['col1']) \
            .map(lambda x: {'col4': x['col1'], 'col5': x['col2'] * x['col3']})\
            .collect()
        assert res == [(1, 3), (4, 6), (7, 9)]

        res = c.parallelize(ints) \
            .map(lambda x: {'col1': x[0], 'col2': x[1], 'col3': x[2]}) \
            .mapColumn('col1', lambda x: 1.2 * x) \
            .withColumn('col1', lambda x: x['col1'] + x['col3']) \
            .mapColumn('col2', lambda x: str(x))\
            .collect()
        assert res == [(4.2, '2', 3), (10.8, '5', 6), (17.4, '8', 9)]

        floats = [(1.2, 3.4), (5.6, 7.8), (9.0, 0.1)]
        res = c.parallelize(floats) \
            .map(lambda x: {'col1': x[0], 'col2': x[1], 'col3': x[0] + x[1]}) \
            .withColumn('col2', lambda x: x['col2'] - x['col1']) \
            .map(lambda x: {'col4': x['col1'], 'col5': x['col2'] * x['col3']}) \
            .collect()
        assert res == [(1.2, 10.12), (5.6, 29.48), (9.0, -80.99)]

        res = c.parallelize(floats) \
            .map(lambda x: {'col1': x[0], 'col2': x[1]}) \
            .mapColumn('col1', lambda x: 0.5 * x) \
            .withColumn('col4', lambda x: (x['col1'] > 3)) \
            .mapColumn('col2', lambda x: int(x)) \
            .collect()
        assert res == [(0.6, 3, False), (2.8, 7, False), (4.5, 0, True)]

    # test the dictionary return to Python
    def test_return(self):
        c = Context(self.conf)
        strings = [('hello', 'world', 'hi'), ('foo', 'bar', 'baz'), ('blank', '', 'not')]
        ints = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        floats = [(1.2, 3.4, -100.2), (5.6, 7.8, -1.234), (9.0, 0.1, 2.3)]
        bools = [(True, False, False), (False, True, True), (False, True, False)]
        for l1 in [strings, ints, floats, bools]:
            for l2 in [strings, ints, floats, bools]:
                l3 = [x + y for x, y in (zip(l1, l2))]
                lam1 = lambda x: ({x[0]: x[3], x[1]: x[4], x[2]: x[5]},)
                lam2 = lam1
                if l1 == bools:  # only two keys
                    l3 = [x[:2] + y[:2] for x, y in zip(l1, l2)]
                    lam1 = lambda x, y, a, b: ({x: a, y: b},)
                    lam2 = lambda x: ({x[0]: x[2], x[1]: x[3]},)

                res = c.parallelize(l3).map(lam1).collect()
                print(list(map(lam2, l3)))
                print(l3)
                print(res)
                assert res == list(map(lam2, l3))

    # test the dictionary parallelize to Python
    def test_parallelize(self):
        c = Context(self.conf)
        strings = [('hello', 'world', 'hi'), ('foo', 'bar', 'baz'), ('blank', '', 'not')]
        ints = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        floats = [(1.2, 3.4, -100.2), (5.6, 7.8, -1.234), (9.0, 0.1, 2.3)]
        bools = [(True, False, False), (False, True, True), (False, True, False)]

        # test first arbitrary dictionaries
        for l1 in [ints, floats, bools]:
            for l2 in [strings, ints, floats, bools]:
                l3 = [{x[0]: y[0], x[1]: y[1], x[2]: y[2]} for x, y in (zip(l1, l2))]
                if l1 == bools:  # only two keys
                    l3 = [x[:2] + y[:2] for x, y in zip(l1, l2)]

                ref = l3

                res = c.parallelize(l3).collect()
                assert res == ref

        # for dictionaries with string keys, the first level gets auto-unpacked.
        for l2 in [strings, ints, floats, bools]:
            l3 = [{'a' : x[0], 'b' : x[1], 'c': x[2]} for x in l2]
            res = c.parallelize(l3).map(lambda x: (x['a'], x['b'], x['c'])).collect()
            assert res == l2


    # on Mac OS X a weird segfault happens. This is the test to debug it.
    # same goed when executing with python3.7 on Ubuntu.
    # i.e. this test triggered a bug because of incorrect ref counting in the Python interpreter
    # left here as test.
    def test_combined(self):
        c = Context(self.conf)
        strings = [('hello', 'world', 'hi'), ('foo', 'bar', 'baz'), ('blank', '', 'not')]
        floats = [(1.2, 3.4, -100.2), (5.6, 7.8, -1.234), (9.0, 0.1, 2.3)]

        # for dictionaries with string keys, the first level gets auto-unpacked.
        l2 = floats
        l3 = [{'a' : x[0], 'b' : x[1], 'c': x[2]} for x in l2]
        res = c.parallelize(l3).map(lambda x: (x['a'], x['b'], x['c'])).collect()
        assert res == l2

        l2 = strings
        for l1 in [strings, floats]:
            l3 = [x + y for x, y in (zip(l1, l2))]
            lam1 = lambda x: ({x[0]: x[3], x[1]: x[4], x[2]: x[5]},)
            lam2 = lam1
            ds = c.parallelize(l3)
            res = ds.map(lam1).collect()
            assert res == list(map(lam2, l3))


    def test_len(self):
        c = Context(self.conf)
        res = c.parallelize([('a', 1, 'b', 2, 'c', 3)]).map(lambda a, x, b, y, c, z: len({a: x, b: y, c: z})).collect()
        assert res == [3]