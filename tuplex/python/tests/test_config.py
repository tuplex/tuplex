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
from tuplex.utils.common import *

class TestConfig(unittest.TestCase):



    def testNestedDictOptions(self):

        c = Context(conf={'executorMemory':'1MB', 'executorCount':3})

        opt = c.options()

        assert 'tuplex.executorMemory' in opt
        assert 'tuplex.executorCount' in opt
        assert opt['tuplex.executorMemory'] == '1MB'
        assert opt['tuplex.executorCount'] == 3

    def testNestedDictOptionsWithKWArgs(self):

        # kwargs override conf
        c = Context(conf={'executorMemory':'1MB', 'executorCount':3}, executorCount=5)

        opt = c.options()

        assert 'tuplex.executorMemory' in opt
        assert 'tuplex.executorCount' in opt
        assert opt['tuplex.executorMemory'] == '1MB'
        assert opt['tuplex.executorCount'] == 5

    def testFileOptions(self):
        c = Context(executorCount=9)

        c.optionsToYAML('test.yaml')

        c2 = Context(conf='test.yaml')

        opt = c2.options()

        assert 'tuplex.executorCount' in opt
        assert opt['tuplex.executorCount'] == 9