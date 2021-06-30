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
import os
from tuplex import *

class TestCSV(unittest.TestCase):

    def _generate_csv_file(self, path, delimiter, has_header=False):
        with open(path, 'w') as csv_file:
            if has_header:
                csv_file.write(delimiter.join(['a', 'b', 'c', 'd']) + '\n')
            for i in range(3):
                elements = []
                for j in range(3):
                    elements.append(str(3 * i + j + 1))
                elements.append('FAST ETL!')
                csv_file.write(delimiter.join(elements) + '\n')

    def setUp(self):
        self._generate_csv_file('test.csv', ',')
        self._generate_csv_file('test.tsv', '\t')
        self._generate_csv_file('test_header.csv', ',', True)
        self.conf = {"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"}

    def tearDown(self):
        os.remove('test.csv')
        os.remove('test.tsv')
        os.remove('test_header.csv')

    def test_show(self):
        c = Context(self.conf)
        dataset = c.csv("test.csv")
        dataset.show()

    def test_map(self):
        c = Context(self.conf)
        dataset = c.csv("test.csv")
        res = dataset.map(lambda a, b, c, d: a).collect()
        assert res == [1, 4, 7]

        dataset = c.csv("test.csv")
        res = dataset.map(lambda a, b, c, d: d).collect()
        assert res == ["FAST ETL!", "FAST ETL!", "FAST ETL!"]

    def test_tsv(self):
        c = Context(self.conf)
        dataset = c.csv("test.tsv", delimiter='\t')
        res = dataset.map(lambda a, b, c, d: a).collect()
        assert res == [1, 4, 7]

        dataset = c.csv("test.tsv", delimiter='\t')
        res = dataset.map(lambda a, b, c, d: d).collect()
        assert res == ["FAST ETL!", "FAST ETL!", "FAST ETL!"]

    def test_non_existent_file(self):
        c = Context(self.conf)
        dataset = c.csv("test.ccc")
        dataset.show()

    def test_csv_with_header(self):
        c = Context(self.conf)
        dataset = c.csv("test_header.csv", header=True)
        res = dataset.map(lambda a, b, c, d: a).collect()
        assert res == [1, 4, 7]


# # test cases for CSV files (in resources folder)
# # same test files as Spark uses
# class TestSparkCSVFiles(unittest.TestCase):
#     def test_spark(self):
#         fname = 'bool.csv'
#         c = Context()
#         ds = c.csv(fname)
#         res = ds.collect()
#         assert len(res) == 3
#         assert res == [True, False, True]