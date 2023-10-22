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
from helper import options_for_pytest

# this test is addressed on issues with the framework usage. I.e. whether data is kept correctly
# in memory for parallelize
class TestMultiStatements(unittest.TestCase):

    def setUp(self):
        self.conf = options_for_pytest()
        self.conf.update({"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"})

    def testParallelize(self):
        c = Context(self.conf)
        ds = c.parallelize([1, 2, 3, 4, 5])
        res1 = ds.map(lambda x: x * x)
        assert res1.collect() == [1, 4, 9, 16, 25]

        res2 = ds.map(lambda x: x * x * x)
        assert res2.collect() == [1, 8, 27, 64, 125]

        assert ds.collect() == [1, 2, 3, 4, 5]