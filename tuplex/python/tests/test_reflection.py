#!/usr/bin/env python3
# ----------------------------------------------------------------------------------------------------------------------#
#                                                                                                                      #
#                                       Tuplex: Blazing Fast Python Data Science                                       #
#                                                                                                                      #
#                                                                                                                      #
#  (c) 2017 - 2021, Tuplex team                                                                                        #
#  Created by Leonhard Spiegelberg first on 1/1/2021                                                                   #
#  License: Apache 2.0                                                                                                 #
# ----------------------------------------------------------------------------------------------------------------------#

from unittest import TestCase

from tuplex.utils.reflection import get_source, get_globals, supports_lambda_closure
from notebook_utils import get_jupyter_function_code

# check reflection on direct import
import udf_direct_import

# check reflection for import into namespace
from udf_from_import import f as udf_f
from udf_from_import import g as udf_g

SOME_CONSTANT_TO_EXTRACT = 42


## tests for lambda functions
class TestReflection(TestCase):

    def get_src(self, func, expected_src):
        # (1) file mode
        self.assertEqual(get_source(func), expected_src)

        # (2) jupyter notebook
        # check if function or lambda
        if func.__name__ == '<lambda>':
            assert expected_src.startswith('lambda')
            res = get_jupyter_function_code('f', 'f = ' + expected_src)
            self.assertEqual(res, expected_src)
        else:
            res = get_jupyter_function_code(func.__name__, expected_src)
            self.assertEqual(expected_src, res)

    def test_simple_lambda(self):
        # simple lambda
        self.get_src(lambda x: x * x, "lambda x: x * x")

    def test_declared_lambda(self):
        # note that the code getter formats the code.
        g = lambda x: x ** 2

        self.assertEqual(get_source(g), 'lambda x: x ** 2')

    def test_declared_nested_lambda(self):

        def test():
            # note that the code getter formats the code.
            g = lambda x: x ** 2

            return get_source(g)

        self.assertEqual(test(), 'lambda x: x ** 2')

    def test_multi_param_lambda(self):
        # lambda with more than one parameter
        self.get_src(lambda a, b, c, d: a + b, "lambda a, b, c, d: a + b")

    def test_noparam_lambda(self):
        # lambda with no parameter
        self.get_src(lambda: 10, "lambda: 10")

    def test_linebreak_lambda(self):
        # lambda with line break
        self.get_src(lambda t: t['geo'][0] \
            ['hello'], "lambda t: t['geo'][0]['hello']")

    def test_multilinestr_lambda(self):
        self.get_src(lambda t: 'hello ' \
                               'world', 'lambda t: \'hello world\'')

        self.get_src(lambda t: 'hello ''world', 'lambda t: \'hello world\'')

        # " quotes are converted to '.
        self.get_src(lambda t: "hello", 'lambda t: \'hello\'')

    def test_multilinestr_raw_lambda(self):
        # multiline string get converted to standard string representation
        self.get_src(lambda t: """hello
world""", "lambda t: 'hello\\nworld'")

    def test_globalextract_func(self):
        g = 20

        def f(x):
            return x + g

        globs = get_globals(f)
        self.assertIn('g', globs)

    def test_globalextract_lam(self):
        g = 43
        f = lambda x: x + g
        globs = get_globals(f)
        self.assertIn('g', globs)

    def test_moduleglob(self):
        def f(x):
            if x > 20:
                return SOME_CONSTANT_TO_EXTRACT * x
            else:
                return 0

        g = lambda x: SOME_CONSTANT_TO_EXTRACT

        globs_f = get_globals(f)
        globs_g = get_globals(g)

        self.assertIn('SOME_CONSTANT_TO_EXTRACT', globs_f)
        self.assertIn('SOME_CONSTANT_TO_EXTRACT', globs_g)

    def test_withindictextract(self):
        local_g = 400

        def f(x):
            return {SOME_CONSTANT_TO_EXTRACT: 'var', x: 'var'}

        g = lambda x: {x: 'v', SOME_CONSTANT_TO_EXTRACT: 'w', local_g: 'u'}

        globs_f = get_globals(f)
        globs_g = get_globals(g)

        self.assertIn('SOME_CONSTANT_TO_EXTRACT', globs_f)
        self.assertIn('SOME_CONSTANT_TO_EXTRACT', globs_g)
        self.assertIn('local_g', globs_g)

    def test_indexingextract(self):
        NUM_COL_IDX = 1
        f = lambda x: int(x[NUM_COL_IDX])

        # check that globals are detected
        f_globs = get_globals(f)
        self.assertIn('NUM_COL_IDX', f_globs)
        self.assertTrue(len(f_globs) != 0)

        # extraction of source code for lambdas only works in patched interpreter when
        # there are multiple lambda experessions per line, however for a single lambda per line, Tuplex can get the code.
        # an interesting read regarding this is https://code.activestate.com/recipes/578353-code-to-source-and-back/
        print('lambda globals supported, testing source extract.')
        f_src = get_source(f)
        self.assertEqual(f_src, 'lambda x: int(x[NUM_COL_IDX])')


#### tests for functions


# test function defined here
def extract_test_Func(s):
    import re
    return re.match('hello', s)


class TestPythonFileMode(TestCase):
    def cmp_code(self, a, b):
        # convert tabs into 4 spaces for edit
        self.assertEqual(a.replace('\t', ' ' * 4), b.replace('\t', ' ' * 4))

    def test_nested_function(self):
        """test whether functions defined via def ... are extracted correctly"""

        ## !!!! Leave this file formatted with tab == 4 spaces !!!!
        def nested_functionI(x, y):
            return x + y

        self.cmp_code(get_source(nested_functionI), 'def nested_functionI(x, y):\n\treturn x + y')

    def test_upper_scope_function(self):
        self.cmp_code(get_source(extract_test_Func),
                      'def extract_test_Func(s):\n\timport re\n\treturn re.match(\'hello\', s)')

    def test_lambda_within_loop(self):
        # check that reflection within a loop works...
        for y in range(0, 10):
            code = get_source(lambda x: x + y)
            self.assertEqual(code, "lambda x: x + y")
            f = lambda x: x + y
            code = get_source(f)
            self.assertEqual(code, "lambda x: x + y")
            # defined within if condition
            if y % 2 == 0:
                g = lambda x: x + y
                code = get_source(g)
                self.assertEqual(code, "lambda x: x + y")
            # below is super tricky and nearly impossible to achieve without patched python interpreter.
            # # defined within if-else expression
            # h = lambda x: x + y if y % 2 == 0 else lambda x: x - y
            # if y % 2 == 0:
            #     code = get_source(h)
            #     self.assertEqual(code, "lambda x: x + y")
            # else:
            #     code = get_source(h)
            #     self.assertEqual(code, "lambda x: x - y")

    def test_def_within_loop(self):
        for y in range(0, 10):
            def f(x, y):
                return x + y

            code = get_source(f)
            self.assertEqual(code.replace('\t', ' ' * 4), "def f(x, y):\n    return x + y")

    # # other test cases to watch out for:
    # G = 100
    # g = lambda x: x + G
    # f = lambda x: x + G

    def test_docstring_nested_function(self):

        def nested_function(x, y):
            """
            This function has some stupid docstring here
            Args:
                x:
                y:

            Returns:

            """
            return x + y

        expected_src = 'def nested_function(x, y):\n\t"""\n\tThis function has some stupid docstring here' \
                       '\n\tArgs:\n\t\tx:\n\t\ty:\n\n\tReturns:\n\n\t"""\n\treturn x + y'
        self.cmp_code(get_source(nested_function), expected_src)

    def test_other_module(self):
        code = get_source(udf_direct_import.f)
        ref_code = "lambda x: x * x"
        self.assertEqual(code, ref_code)

        code = get_source(udf_direct_import.g)
        ref_code = "def g(x):\n" \
                   "    return x + (x * 1)"
        self.assertEqual(code, ref_code)

    def test_other_from_module(self):
        code = get_source(udf_f)
        ref_code = "lambda x: x * x"
        self.assertEqual(code, ref_code)

        code = get_source(udf_g)
        ref_code = "def g(x):\n" \
                   "    return x + (x * 1)"
        self.assertEqual(code, ref_code)
