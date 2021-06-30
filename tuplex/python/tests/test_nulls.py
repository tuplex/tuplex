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


class TestNulls(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        self.conf = {"webui.enable" : False, "driverMemory" : "16MB", "partitionSize" : "256KB"}
        super(TestNulls, self).__init__(*args, **kwargs)

    def testEqAndNotEq(self):
        c = Context(self.conf)
        ds = c.parallelize([1, None])

        # mixed types, i.e. Option[i64] vs. null
        res = ds.map(lambda x: x == None).collect()
        assert res == list(map(lambda x: x == None, [1, None]))

        res = ds.map(lambda x: x != None).collect()
        assert res == list(map(lambda x: x != None, [1, None]))

        # null vs null
        res = c.parallelize([None, None]).map(lambda x: x == None).collect()
        assert res == [True, True]
        res = c.parallelize([None, None]).map(lambda x: x != None).collect()
        assert res == [False, False]

        # null vs i64
        res = c.parallelize([None, None]).map(lambda x: x == 42).collect()
        assert res == [False, False]
        res = c.parallelize([None, None]).map(lambda x: x != 42).collect()
        assert res == [True, True]

        del c