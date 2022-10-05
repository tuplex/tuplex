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
from .helper import test_options

# test filter functionality
class TestFilter(unittest.TestCase):

    def setUp(self):
        self.conf = test_options()
        self.conf.update({"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"})

    def testFilter(self):
        c = Context(self.conf)
        ds = c.parallelize([1, 2, 3, 4, 5])
        res1 = ds.map(lambda x: x * x).filter(lambda x: x > 10)
        assert res1.collect() == [16, 25]

        res2 = ds.filter(lambda x: x == 2 or x == 3 or x == 5).map(lambda x: x * x * x)
        assert res2.collect() == [8, 27, 125]

        assert ds.filter(lambda x: 2 < x <= 4).collect() == [3, 4]

    def testFilterAll(self):
        c = Context(self.conf)
        ds = c.parallelize([1, 2, 3, 4, 5])
        res = ds.filter(lambda x: x > 10).collect()
        assert res == []