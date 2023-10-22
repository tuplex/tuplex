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

import typing
import unittest
from tuplex import *
from helper import options_for_pytest

# test filter functionality
class TestInspection(unittest.TestCase):

    def setUp(self):
        self.conf = options_for_pytest()
        self.conf.update({"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"})

    def testTypes(self):
        """ test .types property """
        c = Context(self.conf)

        t0 = c.parallelize([(1, 2, None), (2, 3, 4.5)]).types
        assert t0 == [int, int, typing.Optional[float]]

        t1 = c.parallelize([None, None, None]).types
        assert t1 == [type(None)]

        t2 = c.parallelize([[1, 2, 3], [3, 4, 5]]).types
        assert t2 == [typing.List[int]]

        t3 = c.parallelize([((), 1, 'hello', False, 4.6, ({'key' : 30}, 20))]).types
        assert t3 == [(), int, str, bool, float, (typing.Dict[str, int], int)]