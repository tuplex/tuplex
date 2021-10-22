from unittest import TestCase
import tuplex
class TestIs(TestCase):

    def setUp(self):
        self.c = tuplex.Context(webui=False)

    def test_boolIsBool(self):
        res = self.c.parallelize([False, True, False, False, True]).map(lambda x: x is False).collect()
        self.assertEqual(res, [True, False, True, True, False])

    def test_boolIsNotBool(self):
        res = self.c.parallelize([True, False, True, False, True]).map(lambda x: x is not False).collect()
        self.assertEqual(res, [True, False, True, False, True])

    def test_boolIsNone(self):
        res = self.c.parallelize([True, False, False, True]).map(lambda x: x is None).collect()
        self.assertEqual(res, [False] * 4)

    def test_mixedIsNotNone(self):
        res = self.c.parallelize([None, True, False]).map(lambda x: x is not None).collect()
        self.assertEqual(res, [False, True, True])

    # def test_throwError(self):
    #     res = self.c.parallelize(["hi", 0.5, {'a': 0}]).map(lambda x: x is not True).collect()
    #     self.assertRaises(TypeError)
