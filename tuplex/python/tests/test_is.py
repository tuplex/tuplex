from unittest import TestCase
import tuplex
class TestIs(TestCase):

    def setUp(self):
        self.conf = {"webui.enable": False, "driverMemory": "64MB", "executorMemory": "2MB", "partitionSize": "128KB"}
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

    # cannot put None here, because None becomes 
    def test_mixedIsNotNone(self):
        res = self.c.parallelize([None, None, None]).map(lambda x: x is not None).collect()
        self.assertEqual(res, [False, False, False])

    # this test fails for none, probably has to do with normal case. 
    def test_mixedIsNotNone2(self):
        res = self.c.parallelize([None, True, False]).map(lambda x: x is not None).collect()
        self.assertEqual(res, [False, True, True])
