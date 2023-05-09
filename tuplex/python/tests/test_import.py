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
import tuplex
from helper import test_options

import re

from udf_from_import import f as udf_f

class Test_Import(unittest.TestCase):
    def test_import(self):
        c = tuplex.Context(test_options())

        # map
        res = c.parallelize(['123', 'abc']).map(lambda x: re.search('\\d+', x) != None).collect()
        self.assertEqual(res, [True, False])

        # filter
        res = c.parallelize(['123', 'abc']).filter(lambda x: re.search('\\d+', x) != None).collect()
        self.assertEqual(res, ['123'])

        # mapColumn
        res = c.parallelize(['123', 'abc'], ['col']).mapColumn('col', lambda x: re.search('\\d+', x) != None).collect()
        self.assertEqual(res, [True, False])

        # withColumn
        res = c.parallelize(['123', 'abc'], ['col']).withColumn('result', lambda x: re.search('\\d+', x['col']) != None).collect()
        res = [t[1] for t in res]
        self.assertEqual(res, [True, False])

        # resolve
        NUM_COL_IDX = 1
        res = c.parallelize([(0, '123'), (12, 'abc')]).filter(lambda a, b: 10 / a > len(b)) \
            .resolve(ZeroDivisionError, lambda x: re.search('\\d+', x[1]) != None) \
            .map(lambda x: int(x[NUM_COL_IDX])).collect()
        self.assertEqual(res, [123])

    def test_external(self):
        c = tuplex.Context(test_options())

        res = c.parallelize([1, 2, 3, 4]).map(udf_f).collect()

        self.assertEqual(res, [1, 4, 9, 16])

if __name__ == '__main__':
    unittest.main()