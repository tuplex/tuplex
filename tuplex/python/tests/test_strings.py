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
from helper import test_options

class TestString(unittest.TestCase):

    def setUp(self):
        self.conf = test_options()
        self.conf.update({"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"})

    def test_concat(self):
        c = Context(self.conf)

        res = c.parallelize([
            ("hello", "world"),
            ("foo", "bar"),
            ("blank", ""),
            ("", "another"),
            ("", "")
        ]).map(lambda a, b: a + b).collect()
        assert res == ["helloworld", "foobar", "blank", "another", ""]

    def test_duplication(self):
        c = Context(self.conf)

        res = c.parallelize([
            ("negative", -2),
            ("zero", 0),
            ("hello", 1),
            ("goodbye", 5)
        ]).map(lambda a, b: a * b).collect()
        assert res == ["", "", "hello", "goodbyegoodbyegoodbyegoodbyegoodbye"]

        res = c.parallelize([
            (-2, "negative"),
            (0, "zero"),
            (1, "hello"),
            (6, "foo")
        ]).map(lambda a, b: a * b).collect()
        assert res == ["", "", "hello", "foofoofoofoofoofoo"]

        res = c.parallelize([
            (True, "true"),
            (False, "false"),
        ]).map(lambda a, b: a * b).collect()
        assert res == ["true", ""]

        res = c.parallelize([
            ("false", False),
            ("true", True),
        ]).map(lambda a, b: a * b).collect()
        assert res == ["", "true"]

    def test_slice(self):
        c = Context(self.conf)

        teststr = "hello"
        indices = [-10, -2, 3, 1, 10]
        pairs = [(-10, 10), (-10, -2), (10, -2), (-10, 2), (2, -10), (-10, 3), (-2, 10), (-2, -1), (-3, 4), (1, 10),
                 (1, -3), (1, 3)]
        triples = [(-10, 10, 2), (1, 10, 2), (1, 4, 2), (10, -10, -2), (10, 1, -2), (4, 1, -2)]

        # single index
        to_test = [(teststr, x) for x in indices]

        res = c.parallelize(to_test).map(lambda s, i: s[i:]).collect()
        assert len(res) == len(to_test)
        assert res == list(map(lambda x: x[0][x[1]:], to_test))

        res = c.parallelize(to_test).map(lambda s, i: s[:i]).collect()
        assert len(res) == len(to_test)
        assert res == list(map(lambda x: x[0][:x[1]], to_test))

        res = c.parallelize(to_test).map(lambda s, i: s[::i]).collect()
        assert len(res) == len(to_test)
        assert res == list(map(lambda x: x[0][::x[1]], to_test))

        # two indices
        to_test = [(teststr, x, y) for x, y in pairs]

        res = c.parallelize(to_test).map(lambda a, b, y: a[b:y]).collect()
        assert len(res) == len(to_test)
        assert res == list(map(lambda x: x[0][x[1]:x[2]], to_test))

        res = c.parallelize(to_test).map(lambda a, b, y: a[:b:y]).collect()
        assert len(res) == len(to_test)
        assert res == list(map(lambda x: x[0][:x[1]:x[2]], to_test))

        res = c.parallelize(to_test).map(lambda a, b, y: a[b::y]).collect()
        assert len(res) == len(to_test)
        assert res == list(map(lambda x: x[0][x[1]::x[2]], to_test))

        # three indices
        to_test = [(teststr, x, y, z) for x, y, z in triples]

        res = c.parallelize(to_test).map(lambda a, x, y, z: a[x:y:z]).collect()
        assert len(res) == len(to_test)
        assert res == list(map(lambda x: x[0][x[1]:x[2]:x[3]], to_test))

    def test_strcast(self):
        c = Context(self.conf)

        # this bug only occurs when we have the ordering False, True
        test = [(False, True)]
        res = c.parallelize(test).map(lambda x: (x[0], x[1])).collect()
        assert res == [(False, True)]
        res = c.parallelize(test).map(lambda x, y: (str(x), str(y))).collect()
        assert res == [('False', 'True')]
        res = c.parallelize(test).map(lambda x: (str(x[0]), str(x[1]))).collect()
        assert res == [('False', 'True')]


        testsets = [
            [-10, 0, 20],
            [-10.123, 0.0, 1.23, float('inf'), float('nan')],
            ["-10", "hello", "", "   bye   ", "7.123"],
            [True, False]
        ]

        for testset in testsets:
            res = c.parallelize(testset).map(lambda x: str(x)).collect()
            expected_res = list(map(lambda x: str(x), testset))
            assert res == expected_res

    def test_strip(self):
        c = Context(self.conf)

        def test_against_python(f, g, arr):
            if g is None:
                g = f
            res = c.parallelize(arr).map(f).collect()
            assert(len(res) == len(arr))
            assert res == list(map(g, arr))

        def test_tuplex(f, g, arr):
            res1 = c.parallelize(arr).map(f).collect()
            res2 = c.parallelize(arr).map(g).collect()
            assert(len(res1) == len(arr))
            assert(len(res2) == len(arr))
            assert res1 == res2

        to_test1 = [" \n\t\r hello \n\r\r\r  ", "  \t\r\x0b", "goodbye!\n\n\n", " \r\tabcde"]
        to_test2 = [("hello", "hlo"), ("abceeeeeedr", "acedr"), ("lmnop", "p"), ("lmnop", "lm")]

        # check that strip, lstrip, rstrip work
        test_against_python(lambda x: x.strip(), None, to_test1)
        test_against_python(lambda x: x.lstrip(), None, to_test1)
        test_against_python(lambda x: x.rstrip(), None, to_test1)
        test_against_python(lambda x, y: x.strip(y), lambda x: x[0].strip(x[1]), to_test2)
        test_against_python(lambda x, y: x.lstrip(y), lambda x: x[0].lstrip(x[1]), to_test2)
        test_against_python(lambda x, y: x.rstrip(y), lambda x: x[0].rstrip(x[1]), to_test2)

        # test that strip, lstrip(rstrip) do the same
        test_tuplex(lambda x: x.strip(), lambda x: x.lstrip().rstrip(), to_test1)
        test_tuplex(lambda x, y: x.strip(y), lambda x, y: x.lstrip(y).rstrip(y), to_test2)

    def test_startswith(self):
        c = Context(self.conf)
        to_test1 = [("hello", "h"), ("hello", "he"), ("Hello", "hello"), ("abcde", "abcde")]
        res = c.parallelize(to_test1).map(lambda s, p: s.startswith(p)).collect()
        print(res)
        assert(res == [True, True, False, True])

    def test_strconv(self):
        # tests str(...) call

        c = Context(self.conf)

        res = c.parallelize([(None, (), {})]).map(lambda a, b, c: (str(a), str(b), str(c))).collect()
        assert res == [('None', '()', '{}')]

        res = c.parallelize([1, 2, None, 3, None, None]).map(lambda x: str(x)).collect()
        assert res == ['1', '2', 'None', '3', 'None', 'None']

    def test_swapcase(self):
        c = Context(self.conf)

        to_test = ["all lower",
                   "ALL UPPER",
                   "Upper && Lower",
                   "Number *45.67++",
                   "",
                   "pQ" * 16,
                   "LongString!!" * 100 + "\t\t\n\n" + "1234" * 50 + "AbcD" * 40,
                   "100^^**&&" * 100 + " " * 20 + ";;[]" * 20 + "Test"]
        res = c.parallelize(to_test).map(lambda x: x.swapcase()).collect()
        assert res == ["ALL LOWER",
                       "all upper",
                       "uPPER && lOWER",
                       "nUMBER *45.67++",
                       "",
                       "Pq" * 16,
                       "lONGsTRING!!" * 100 + "\t\t\n\n" + "1234" * 50 + "aBCd" * 40,
                       "100^^**&&" * 100 + " " * 20 + ";;[]" * 20 + "tEST"]
        
    def test_strcenter(self):
        c = Context(self.conf)

        def test_against_python(f, g, arr):
            if g is None:
                g = f
            res = c.parallelize(arr).map(f).collect()
            assert(len(res) == len(arr))
            assert res == list(map(g, arr))

        to_test1 = ["", "a", "ab", "abc", "abcd"]
        to_test2 = [("", "|"), ("a", ","), ("ab", "+"), ("abc", "2"), ("abcd", "%"), ("abcde", "*")]
        to_test3 = [("", "|"), ("a", ","), ("ab", "+"), ("abc", "2"), ("abcd", "%"), ("abcde", "*")]

        test_against_python(lambda x: x.center(3), None, to_test1)
        test_against_python(lambda x: x.center(True), None, to_test1)
        test_against_python(lambda x: x.center(False), None, to_test1)
        test_against_python(lambda x, y: x.center(4, y), lambda x: x[0].center(4, x[1]), to_test2)
        test_against_python(lambda x, y: x.center(True, y), lambda x: x[0].center(1, x[1]), to_test3)
        test_against_python(lambda x, y: x.center(False, y), lambda x: x[0].center(0, x[1]), to_test3)