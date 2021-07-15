import unittest
from tuplex import *

# test sort functionality
class TestSort(unittest.TestCase):

    def setUp(self):
        self.conf = {"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"}

    def testSort(self):
        # base case where nothing is passed into sort
        c = Context(self.conf)
        ds = c.parallelize([1, 2, 3, 4, 5])
        res1 = ds.sort()
        print("A", res1.collect())
        assert res1.collect() == [1, 2, 3, 4, 5]

        # base case where by is empty dict
        c = Context(self.conf)
        ds = c.parallelize([1, 2, 3, 4, 5])
        res1 = ds.sort(by=dict())
        print("B", res1.collect())
        assert res1.collect() == [1, 2, 3, 4, 5]

        # no duplicates. 1 col. asc
        c = Context(self.conf)
        ds = c.parallelize([5, 4, 3, 2, 1])
        by = SortBy.ASCENDING
        d = {0:by}
        res1 = ds.sort(by=d)
        print("C", res1.collect())
        assert res1.collect() == [1, 2, 3, 4, 5]

        # no duplicates. 1 col. desc
        c = Context(self.conf)
        ds = c.parallelize([1, 2, 3, 4, 5])
        d = {0:SortBy.DESCENDING}
        res1 = ds.sort(by=d)
        print("D", res1.collect())
        assert res1.collect() == [5, 4, 3, 2, 1]

        # no duplicates. 1 col. length asc
        c = Context(self.conf)
        ds = c.parallelize(["az", "baaz", "aaa"])
        d = {0:SortBy.ASCENDING_LENGTH}
        res1 = ds.sort(by=d)
        print("D", res1.collect())
        assert res1.collect() == ["az", "aaa", "baaz"]

        # no duplicates. 1 col. length desc
        c = Context(self.conf)
        ds = c.parallelize(["az", "baaz", "aaa"])
        d = {0:SortBy.DESCENDING_LENGTH}
        res1 = ds.sort(by=d)
        print("D", res1.collect())
        assert res1.collect() == ["baaz", "aaa", "az"]

        # no duplicates. 1 col. length cast asc
        c = Context(self.conf)
        ds = c.parallelize([19, 111, 5])
        d = {0:SortBy.ASCENDING_LENGTH}
        res1 = ds.sort(by=d)
        print("D", res1.collect())
        assert res1.collect() == [5, 19, 111]

        # no duplicates. 1 col. length cast desc
        c = Context(self.conf)
        ds = c.parallelize([19, 111, 5])
        d = {0:SortBy.DESCENDING_LENGTH}
        res1 = ds.sort(by=d)
        print("D", res1.collect())
        assert res1.collect() == [111, 19, 5]

        # no duplicates. 1 col. lexico cast asc
        c = Context(self.conf)
        ds = c.parallelize([19, 111, 5, 10])
        d = {0:SortBy.ASCENDING_LEXICOGRAPHICALLY}
        res1 = ds.sort(by=d)
        print("D", res1.collect())
        assert res1.collect() == [10, 111, 19, 5]

        # no duplicates. 1 col. lexico cast desc
        c = Context(self.conf)
        ds = c.parallelize([19, 111, 5, 10])
        d = {0:SortBy.DESCENDING_LEXICOGRAPHICALLY}
        res1 = ds.sort(by=d)
        print("D", res1.collect())
        assert res1.collect() == [5, 19, 111, 10]

        # no duplicates. 1 col. lexico asc
        c = Context(self.conf)
        ds = c.parallelize(["aa", "d", "bbb", "c"])
        d = {0:SortBy.ASCENDING_LEXICOGRAPHICALLY}
        res1 = ds.sort(by=d)
        print("D", res1.collect())
        assert res1.collect() == ["aa", "bbb", "c", "d"]