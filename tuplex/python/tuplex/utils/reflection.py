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

import types
import inspect
import re
# ALWAYS import cloudpickle before dill, b.c. of https://github.com/uqfoundation/dill/issues/383
import cloudpickle
import dill
import ast
import weakref
import dis
import opcode
import types
import itertools
import sys

from tuplex.utils.errors import TuplexException
from tuplex.utils.globs import get_globals
from tuplex.utils.source_vault import SourceVault, supports_lambda_closure
from tuplex.utils.common import in_jupyter_notebook, in_google_colab, is_in_interactive_mode

# only export get_source function, rest shall be private.
__all__ = ['get_source', 'get_globals', 'supports_lambda_closure']

def get_jupyter_raw_code(function_name):
    # ignore here unresolved reference
    history_manager = get_ipython().history_manager
    hist = history_manager.get_range()
    regex = r"def\s*{}\(.*\)\s*:[\t ]*\n".format(function_name)
    signature = 'hist = history_manager.get_range()'
    prog = re.compile(regex)

    matched_cells = []
    for session, lineno, inline in hist:
        test_str = inline

        # skip history referring to this
        if signature in inline:
            continue

        if 'get_function_code' in inline:
            continue

        if prog.search(test_str):
            matched_cells.append((session, lineno, inline))

    return matched_cells[-1][2]

def extractFunctionByName(code, func_name, return_linenos=False):
    class FunctionVisitor(ast.NodeVisitor):
        def __init__(self):
            self.lastStmtLineno = 0
            self.funcInfo = []

        def visit_FunctionDef(self, node):

            print(self.lastStmtLineno)
            self.generic_visit(node)
            print(self.lastStmtLineno)

        def visit(self, node):
            funcStartLineno = -1
            if hasattr(node, 'lineno'):
                self.lastStmtLineno = node.lineno
            if isinstance(node, ast.FunctionDef):
                funcStartLineno = node.lineno
            self.generic_visit(node)
            if isinstance(node, ast.FunctionDef):
                self.funcInfo.append({'name': node.name,
                                      'start': funcStartLineno - 1,
                                      'end': self.lastStmtLineno - 1})

    root = ast.parse(code)
    fv = FunctionVisitor()
    fv.visit(root)

    # find function with name
    candidates = filter(lambda x: x['name'] == func_name, fv.funcInfo)

    def indent(s):
        return len(s) - len(s.lstrip(' \t'))

    lines = code.split('\n')
    # find out level
    candidates = map(lambda x: {**x, 'level': indent(lines[x['start']])}, candidates)

    info = sorted(candidates, key=lambda x: x['level'])[0]

    func_code = '\n'.join(lines[info['start']:info['end'] + 1])

    if return_linenos:
        return func_code, info['start'], info['end']
    else:
        return func_code


def extract_function_code(function_name, raw_code):


    # remove greedily up to num_tabs and num_spaces
    def remove_tabs_and_spaces(line, num_tabs, num_spaces):
        t = 0
        s = 0
        pos = 0
        while pos < len(line):
            c = line[pos]
            if c == ' ':
                s += 1
            elif c == '\t':
                t += 1
            else:
                break
            pos += 1

        return ' ' * max(s - num_spaces, 0) + '\t' * max(t - num_tabs, 0) + line[pos:]

    # remove leading spaces / tabs
    assert len(raw_code) >= 1

    # let's first check whether the function starts that needs to be extracted
    regex = r"[\t ]*def\s*{}\(.*\)\s*:[\t ]*\n".format(function_name)
    start_idx = 0
    for match in re.finditer(regex, raw_code, re.MULTILINE):
        start_idx = match.start()
    first_line = raw_code[start_idx:]

    first_line_num_tabs = len(first_line) - len(first_line.lstrip('\t'))
    first_line_num_spaces = len(first_line) - len(first_line.lstrip(' '))


    func_lines = [remove_tabs_and_spaces(line, first_line_num_tabs, first_line_num_spaces) \
                     for line in raw_code[start_idx:].split('\n')]

    # greedily remove for each line tabs/spaces
    out = '\n'.join(func_lines)
    return extractFunctionByName(out, function_name)

def get_function_code(f):
    """ jupyter notebook, retrieve function history """
    assert isinstance(f, types.FunctionType)
    function_name = f.__code__.co_name
    assert isinstance(function_name, str)

    if in_jupyter_notebook() or in_google_colab():
        return extract_function_code(function_name, get_jupyter_raw_code(function_name))
    else:
        if is_in_interactive_mode():
            # need to extract lines from shell
            # import here, avoids also trouble with jupyter notebooks
            from tuplex.utils.interactive_shell import TuplexShell

            # for this to work, a dummy shell has to be instantiated
            # through which all typing occurs. Thus, the history can
            # be properly captured for source code lookup.
            # shell is a borg object, i.e. singleton alike behaviour
            shell = TuplexShell()
            return shell.get_function_source(f)
        else:
            # extract using dill from file
            return extract_function_code(function_name, dill.source.getsource(f))


vault = SourceVault()

def get_source(f):
    """ Jupyter notebook code reflection """

    if isinstance(f, types.FunctionType):

        # lambda function?
        # use inspect module
        # need to clean out lambda...
        if f.__name__ == '<lambda>':
            # interpreter in interactive mode or not?
            # beware jupyter notebook also returns true for interactive mode!
            if is_in_interactive_mode() and not in_jupyter_notebook() and not in_google_colab():

                # import here, avoids also trouble with jupyter notebooks
                from tuplex.utils.interactive_shell import TuplexShell

                # for this to work, a dummy shell has to be instantiated
                # through which all typing occurs. Thus, the history can
                # be properly captured for source code lookup.
                # shell is a borg object, i.e. singleton alike behaviour
                shell = TuplexShell()
                return shell.get_lambda_source(f)
            else:
                # does lambda have globals?
                # if yes, then extract won't work IFF there's more than one lambda per line!
                # => display warning then.
                # => change hashing method...
                f_globs = get_globals(f)
                f_filename = f.__code__.co_filename
                f_lineno = f.__code__.co_firstlineno
                f_colno = f.__code__.co_firstcolno if hasattr(f.__code__, 'co_firstcolno') else None

                # special case: some unknown jupyter magic has been used...
                if (in_jupyter_notebook() or in_google_colab()) and (f_filename == '<timed exec>' or f_filename == '<timed eval>'):
                    raise TuplexException('%%time magic not supported for Tuplex code')

                src_info = inspect.getsourcelines(f)

                vault.extractAndPutAllLambdas(src_info,
                                              f_filename,
                                              f_lineno,
                                              f_colno,
                                              f_globs)
            return vault.get(f, f_filename, f_lineno, f_colno, f_globs)
        else:
            # works always, because functions can be only defined on a single line!
            return get_function_code(f)
    else:

        # TODO: for constants, create dummy source code, i.e. lambda x: 20
        # when desired to retrieve a constant or so!

        return ''