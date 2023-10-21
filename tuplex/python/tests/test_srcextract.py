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

from unittest import TestCase
import tuplex

from tuplex.utils.reflection import get_source, get_globals, supports_lambda_closure
from notebook_utils import get_jupyter_function_code
from helper import options_for_pytest

SOME_CONSTANT_TO_EXTRACT=42

## tests for lambda functions
class TestSourceExtract(TestCase):

    def setUp(self):
        self.conf = options_for_pytest()
        self.conf.update({"webui.enable" : False, "driverMemory" : "8MB", "partitionSize" : "256KB"})
        self.c = tuplex.Context(self.conf)

    def test_singlelam(self):
        res = self.c.parallelize([1, 2, 3, 4]).map(lambda x: x * x).collect()
        self.assertEqual(res, [1, 4, 9, 16])

    def test_multilamperline(self):
        res = self.c.parallelize([1, 2, 3, 4]).map(lambda x: x * x).map(lambda x: x - 1).collect()
        self.assertEqual(res, [0, 3, 8, 15])

    def test_lamwithglobals(self):
        # single lambda using globals per line, should work!
        res = self.c.parallelize([1, 2, 3, 4]).map(lambda x: x + SOME_CONSTANT_TO_EXTRACT).collect()
        self.assertEqual(res, [43, 44, 45, 46])


    def multilam_perline(self):
        res = self.c.parallelize([1, 2, 3, 4]).map(lambda x: x + SOME_CONSTANT_TO_EXTRACT).map(lambda x: -1 * (SOME_CONSTANT_TO_EXTRACT - x)).collect()
        self.assertEqual(res, [1, 2, 3, 4])

    def test_lamwithmultiglobals(self):
        # global per line should work
        res = self.c.parallelize([1, 2, 3, 4]).map(lambda x: x + SOME_CONSTANT_TO_EXTRACT) \
            .map(lambda x: -1 * (SOME_CONSTANT_TO_EXTRACT - x)).collect()
        self.assertEqual(res, [1, 2, 3, 4])

        # now the test with multiple lambdas per line
        # => fails in normal interpreter, but works in patched interpreter!
        g = lambda x: x * 2
        if not hasattr(g.__code__, 'co_firstcolno'):
            self.assertRaises(Exception, self.multilam_perline)
        else:
            self.multilam_perline()


    def bad_multilinelam(self):
        # DO NOT REFORMAT!!!
        res = self.c.parallelize([1, 2, 3, 4]).map(lambda x: x +
                                                             SOME_CONSTANT_TO_EXTRACT +
                                                             3).map(lambda x: x - SOME_CONSTANT_TO_EXTRACT).map(lambda x: x - 3).collect()
        self.assertEqual(res, [1, 2, 3, 4])

    def test_lamwithglobals_multiline(self):
        # DO NOT REFORMAT!!!
        res = self.c.parallelize([1, 2, 3, 4]).map(lambda x: x +
                                                   SOME_CONSTANT_TO_EXTRACT +
                                                   3).map(lambda x: x - SOME_CONSTANT_TO_EXTRACT).collect()
        self.assertEqual(res, [4, 5, 6, 7])

        if not supports_lambda_closure():
            self.assertRaises(Exception, self.bad_multilinelam)
        else:
            self.bad_multilinelam()