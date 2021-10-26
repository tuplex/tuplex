import tuplex
from unittest import TestCase
"""
Tests functionality for `is` keyword. 
"""
class TestIs(TestCase):

    def setUp(self):
        self.conf = {"webui.enable": False, "executorCount": "0"}
        self.c = tuplex.Context(self.conf)

    def test_boolIsBool(self):
        res = self.c.parallelize([False, True, False, False, True]).map(lambda x: x is False).collect()
        self.assertEqual(res, [True, False, True, True, False])

    def test_boolIsNotBool(self):
        res = self.c.parallelize([True, False, True, False, True]).map(lambda x: x is not False).collect()
        self.assertEqual(res, [True, False, True, False, True])

    def test_boolIsNone(self):
        res = self.c.parallelize([True, False, False, True]).map(lambda x: x is None).collect()
        self.assertEqual(res, [False] * 4)

    def test_mixedIsNone(self):
        res = self.c.parallelize([None, 255, 400, False, 2.3]).map(lambda x: x is None).collect()
        self.assertEqual(res, [True, False, False, False, False])

    def test_mixedIsNotNone(self):
        res = self.c.parallelize([None, None, None]).map(lambda x: x is not None).collect()
        self.assertEqual(res, [False, False, False])

    def test_mixedIsNotNone2(self):
        res = self.c.parallelize([None, True, False]).map(lambda x: x is not None).collect()
        self.assertEqual(res, [False, True, True])

    def test_mixedIsNotNone3(self):
        res = self.c.parallelize([2, False, None]).map(lambda x: x is not None).collect()
        self.assertEqual(res, [True, True, False])
