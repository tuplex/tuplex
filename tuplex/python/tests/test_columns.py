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

from unittest import TestCase

import tuplex

class TestColumns(TestCase):

    def setUp(self):
        self.conf = {"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"}
        self.c = tuplex.Context(self.conf)

    def test_withColumnNew(self):
        res = self.c.parallelize([10, 20, 3, 4]).withColumn('newcol', lambda x: 2 * x).collect()
        self.assertEqual(res, [(10, 20), (20, 40), (3, 6), (4, 8)])

    def test_withColumnSame(self):
        res = self.c.parallelize([(1, 'Hello'), (2, 'world')], ['count', 'word']) \
            .withColumn('word', lambda x: x['word'][-1] * x['count']) \
            .collect()

        self.assertEqual([(1, 'o'), (2, 'dd')], res)

    def test_withColumnSameII(self):
        res = self.c.parallelize([(1, 'Hello'), (2, 'world')], ['count', 'word']) \
            .withColumn('word', lambda x: x[1][-1] * x[0]) \
            .collect()

        self.assertEqual(res, [(1, 'o'), (2, 'dd')])

    def test_mapColumnSingleColumn(self):
        res = self.c.parallelize([1, 2, 3], columns=['A']).mapColumn('A', lambda x: x + 1).collect()
        self.assertEqual(res, [2, 3, 4])

    def test_selectColumns(self):
        ds = self.c.parallelize([(1, 2, 3), (4, 5, 6), (7, 8, 9)], columns=['abc', 'def', 'ghi'])

        res1 = ds.selectColumns(['abc', 'ghi']).collect()
        self.assertEqual(res1, [(1, 3), (4, 6), (7, 9)])
        res2 = ds.selectColumns(['abc',]).collect()
        self.assertEqual(res2, [1, 4, 7])
        res3 = ds.selectColumns(['def']).collect()
        self.assertEqual(res3, [2, 5, 8])
        resAll = ds.selectColumns(['abc', 'def', 'ghi']).collect()
        self.assertEqual(resAll, [(1, 2, 3), (4, 5, 6), (7, 8, 9)])

    def test_selectColumnsII(self):
        # select columns also supports integer (incl. neg indices!) indexing and strings. Can be mixed!
        ds = self.c.parallelize([(1, 2, 3), (4, 5, 6), (7, 8, 9)], columns=['abc', 'def', 'ghi'])

        # single column / index syntax
        resS1 = ds.selectColumns('abc').collect()
        self.assertEqual(resS1, [1, 4, 7])
        resS2 = ds.selectColumns(2).collect()
        self.assertEqual(resS2, [3, 6, 9])
        resS3 = ds.selectColumns(-2).collect()
        self.assertEqual(resS3, [2, 5, 8])

        # doubling
        res1 = ds.selectColumns(['abc', 'abc']).collect()
        self.assertEqual(res1, [(1, 1), (4, 4), (7, 7)])

        # integer indexing
        res2 = ds.selectColumns([1, 0]).collect()
        self.assertEqual(res2, [(2, 1), (5, 4), (8, 7)])

        # mixed indexing incl. negative indices
        res3 = ds.selectColumns([-1, 'def', 'ghi']).collect()
        self.assertEqual(res3, [(3, 2, 3), (6, 5, 6), (9, 8, 9)])

    def test_withColumnUnnamed(self):
        res = self.c.parallelize([(1, 2), (3, 2)]) \
                  .withColumn('newcol', lambda a, b: (a + b)/10) \
                  .collect()

        self.assertEqual(res, [(1, 2, 3/10), (3, 2, 5/10)])