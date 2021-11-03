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
import numpy as np

# test fallback functionality, i.e. executing cloudpickled code
class TestFallback(unittest.TestCase):

    def setUp(self):
        self.conf = {"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"}
        self.c = Context(self.conf)

    def testArbitaryObjecsts(self):
        res = self.c.parallelize([(1, np.zeros(2)), (4, np.zeros(5))]).map(lambda a, b: (a + 1, b)).collect()

        self.assertEqual(len(res), 2)

        self.assertEqual(type(res[0][1]), type(np.zeros(2)))
        self.assertEqual(type(res[1][1]), type(np.zeros(5)))
        self.assertEqual(res[0][0], len(res[0][1]))
        self.assertEqual(res[1][0], len(res[1][1]))

    def testNonSupportedPackage(self):
        # use here numpy as example and perform some operations via numpy, mix with compiled code!

        res = self.c.parallelize([1, 2, 3, 4]).map(lambda x: [x, x*x, x*x*x]) \
                    .map(lambda x: (np.array(x).sum(), np.array(x).mean())).collect()

        self.assertEqual(len(res), 4)

        ref = list(map(lambda x: (np.array(x).sum(), np.array(x).mean()), map(lambda x: [x, x*x, x*x*x], [1, 2, 3, 4])))

        for i in range(4):
            self.assertAlmostEqual(res[i][0], ref[i][0])
            self.assertAlmostEqual(res[i][1], ref[i][1])

    def allSamplesAreNormalCaseViolationUDF(self, x):
        t = 0
        if x == 1:
            t = 1.0
        else:
            t = 'a'
        if x == 2:
            t = 2.0
        else:
            t = 'b'
        if x == 3:
            t = 3.0
        else:
            t = 4.0
        return t

    def testAllSamplesAreNormalCaseViolationUDF(self):
        res = self.c.parallelize([1, 2, 3]).map(self.allSamplesAreNormalCaseViolationUDF).collect()
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0], self.allSamplesAreNormalCaseViolationUDF(1))
        self.assertEqual(res[0], self.allSamplesAreNormalCaseViolationUDF(2))
        self.assertEqual(res[0], self.allSamplesAreNormalCaseViolationUDF(3))
