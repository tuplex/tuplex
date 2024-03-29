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
from helper import options_for_pytest

import re

class Test_Import(unittest.TestCase):
    def test_import(self):
        c = tuplex.Context(options_for_pytest())

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

if __name__ == '__main__':
    unittest.main()