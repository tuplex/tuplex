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
from .helper import test_options

class TestLists(unittest.TestCase):

    def setUp(self):
        self.conf = test_options()
        self.conf.update({"webui.enable" : False, "driverMemory" : "16MB", "partitionSize" : "256KB"})

    def test_subscripts(self):
        c = Context(self.conf)

        # lot of type stress testing
        strings = [('hello', 'world', 'hi'), ('foo', 'bar', 'baz'), ('blank', '', 'not')]
        ints = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        floats = [(1.2, 3.4, -100.2), (5.6, 7.8, -1.234), (9.0, 0.1, 2.3)]
        bools = [(True, False, False), (False, True, True), (False, True, False)]
        nulls = [(None, None, None), (None, None, None), (None, None, None)]
        emptytuples = [((), (), ()), ((), (), ()), ((), (), ())]
        emptydicts = [({}, {}, {}), ({}, {}, {}), ({}, {}, {})]
        emptylists = [([], [], []), ([], [], []), ([], [], [])]
        for l in [strings, ints, floats, bools, nulls, emptytuples, emptydicts, emptylists]:
            res = c.parallelize(l).map(lambda x, y, z: [x, y, z]).map(lambda x: (x[0], x[1], x[2])).collect()
            assert res == l

    # test list return to Python
    def test_return(self):
        c = Context(self.conf)
        strings = [('hello', 'world', 'hi'), ('foo', 'bar', 'baz'), ('blank', '', 'not')]
        ints = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        floats = [(1.2, 3.4, -100.2), (5.6, 7.8, -1.234), (9.0, 0.1, 2.3)]
        bools = [(True, False, False), (False, True, True), (False, True, False)]
        nulls = [(None, None, None), (None, None, None), (None, None, None)]
        emptytuples = [((), (), ()), ((), (), ()), ((), (), ())]
        emptydicts = [({}, {}, {}), ({}, {}, {}), ({}, {}, {})]
        emptylists = [([], [], []), ([], [], []), ([], [], [])]
        for l in [strings, ints, floats, bools, nulls, emptytuples, emptydicts, emptylists]:
            res = c.parallelize(l).map(lambda x, y, z: [x, y, z]).collect()
            assert list(map(lambda x: [x[0], x[1], x[2]], l)) == res
            res = c.parallelize(l).map(lambda x, y, z: ([x, y], [z], [z, y])).collect()
            assert list(map(lambda x: ([x[0], x[1]], [x[2]], [x[2], x[1]]), l)) == res

        res = c.parallelize([1, 2, 3]).map(lambda x: []).collect()
        assert [[], [], []] == res

    # test list parallelize to Python
    def test_parallelize(self):
        c = Context(self.conf)
        strings = [('hello', 'world', 'hi'), ('foo', 'bar', 'baz'), ('blank', '', 'not')]
        ints = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        floats = [(1.2, 3.4, -100.2), (5.6, 7.8, -1.234), (9.0, 0.1, 2.3)]
        bools = [(True, False, False), (False, True, True), (False, True, False)]
        nulls = [(None, None, None), (None, None, None), (None, None, None)]
        emptytuples = [((), (), ()), ((), (), ()), ((), (), ())]
        emptydicts = [({}, {}, {}), ({}, {}, {}), ({}, {}, {})]
        emptylists = [([], [], []), ([], [], []), ([], [], [])]
        for l in [strings, ints, floats, bools, nulls, emptytuples, emptydicts, emptylists]:
            # just parallelize and return a list
            ll = [(list(x),) for x in l]
            res = c.parallelize(ll).map(lambda x: x).collect()
            assert [list(x) for x in l] == res
            # parallelize and return multiple lists
            ll = [(list(x[:2]), list(x)) for x in l]
            res = c.parallelize(ll).map(lambda x: x).collect()
            assert ll == res
            # parallelize over lists, subscript, and return new list
            ll = [(list(x[:2]), list(x)) for x in l]
            res = c.parallelize(ll).map(lambda x, y: [x[0], y[0], x[1]]).collect()
            exres = list(map(lambda x: [x[0][0], x[1][0], x[0][1]], ll))
            assert exres == res

    def test_comprehension(self):
        c = Context(self.conf)

        ints = [0, 1, 2, 5]
        res = c.parallelize(ints).map(lambda x: [t for t in range(x)]).collect()
        assert res == list(map(lambda x: [t for t in range(x)], ints))

        strings = ['this', 'is', 'a', 'test']
        res = c.parallelize(strings).map(lambda x: [2*t for t in x]).collect()
        assert res == list(map(lambda x: [2*t for t in x], strings))

        lists = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        res = c.parallelize(lists).map(lambda x: [t*t for t in x]).collect()
        assert res == list(map(lambda x: [t*t for t in x], lists))

        tuples = [('test', 'is', 'test'), ('good', 'hello', 'hi'), ('7', '8', '9')]
        res = c.parallelize(tuples).map(lambda x: [2*t for t in x]).collect()
        assert res == list(map(lambda x: [2*t for t in x], tuples))

    def test_in(self):
        c = Context(self.conf)

        strings = [('hello', 'world', 'hi'), ('foo', 'bar', 'baz'), ('blank', '', 'not')]
        for t in strings:
            res = c.parallelize(list(t)).map(lambda x: x in ['hello', 'hi', 'bar', '']).collect()
            assert res == list(map(lambda x: x in ['hello', 'hi', 'bar', ''], t))

        ints = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        for t in ints:
            res = c.parallelize(list(t)).map(lambda x: x in [1, 4, 6, 9]).collect()
            assert res == list(map(lambda x: x in [1, 4, 6, 9], t))

        floats = [(1.2, 3.4, -100.2), (5.6, 7.8, -1.234), (9.0, 0.1, 2.3)]
        for t in floats:
            res = c.parallelize(list(t)).map(lambda x: x in [3.4, -100.2, -1.234, 0.1]).collect()
            assert res == list(map(lambda x: x in [3.4, -100.2, -1.234, 0.1], t))

        bools = [(True, False, False), (False, True, True), (False, True, False)]
        for t in bools:
            res = c.parallelize(list(t)).map(lambda x: x in [True, True]).collect()
            assert res == list(map(lambda x: x in [True, True], t))

        nulls = [(None, None, None), (None, None, None), (None, None, None)]
        for t in bools:
            res = c.parallelize(list(t)).map(lambda x: x in [None, None]).collect()
            assert res == list(map(lambda x: x in [None, None], t))

        emptytuples = [((), (), ()), ((), (), ()), ((), (), ())]
        for t in emptytuples:
            res = c.parallelize(list(t)).map(lambda x: x in [()]).collect()
            assert res == list(map(lambda x: x in [()], t))

        emptydicts = [({}, {}, {}), ({}, {}, {}), ({}, {}, {})]
        for t in emptydicts:
            res = c.parallelize(list(t)).map(lambda x: x in [{}]).collect()
            assert res == list(map(lambda x: x in [{}], t))

        emptylists = [([], [], []), ([], [], []), ([], [], [])]
        for t in emptylists:
            res = c.parallelize(list(t)).map(lambda x: x in [[]]).collect()
            assert res == list(map(lambda x: x in [[]], t))