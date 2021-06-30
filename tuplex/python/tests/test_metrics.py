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

# this test is a basic test to make sure that times/exceptions for a project are
# working correctly
class TestMetrics(unittest.TestCase):

    def testTimes(self):
        conf = {"tuplex.useLLVMOptimizer" : "true", "webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"}
        c = Context(conf)
        c.parallelize([1, 2, 3, 4, 5]).map(lambda x: x + 4).collect()
        metrics = c.metrics
        logicalOptimizationTime = metrics.logicalOptimizationTime
        LLVMOptimizationTime = metrics.LLVMOptimizationTime
        LLVMCompilationTime = metrics.LLVMCompilationTime
        totalCompilationTime = metrics.totalCompilationTime
        assert logicalOptimizationTime > 0.0
        assert LLVMOptimizationTime > 0.0
        assert LLVMCompilationTime > 0.0
        assert totalCompilationTime > 0.0