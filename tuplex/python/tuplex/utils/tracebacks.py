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

import traceback
import linecache
import re
from .reflection import get_source

__all__ = ['traceback_from_udf']

def format_traceback(tb, function_name):
    """
    helper function to format a traceback object with line numbers relative to function definition
    Args:
        tb:
        function_name:

    Returns:

    """

    fnames = set()
    out = ''

    for frame, lineno in traceback.walk_tb(tb):
        co = frame.f_code
        filename = co.co_filename
        name = co.co_name
        fnames.add(filename)
        linecache.lazycache(filename, frame.f_globals)
        f_locals = frame.f_locals
        line = linecache.getline(filename, lineno).strip()

        # @Todo: maybe this is faster possible when strip is ignored, by counting tabs or so
        # find base number, substract to get relative line number correct
        start_lineno = lineno
        lineno_correction = 0

        # need here open match for line breaks in function definition.
        # note the use of ^ to make sure docstrings are not matched wrongly
        regex = r"^[\t ]*def\s*{}\(.*".format(function_name)
        while not re.match(regex, linecache.getline(filename, start_lineno).strip()) and start_lineno > 0:
            start_lineno -= 1
        # get line where function def starts via
        # linecache.getline(filename, start_lineno).strip()

        # UI is currently formatted with line numbering starting at 1
        lineno_correction = -start_lineno + 1

        out += 'line {}, in {}:'.format(lineno + lineno_correction, function_name)
        out += '\n\t{}'.format(line)
    for filename in fnames:
        linecache.checkcache(filename)

    return out

# get traceback from sample
def traceback_from_udf(udf, x):
    """
    get a formatted traceback as string by executing a udf over a sample
    Args:
        udf: A function or lambda to be executed with x as argument
        x: argument to pass along to produce traceback

    Returns:
        string with nicely formatted traceback
    """
    # two cases:
    # (1) function

    fname = udf.__name__
    try:
        udf(x)
    except Exception as e:
        assert e.__traceback__.tb_next  # make sure no exception within this function was raised

        etype_name = type(e).__name__
        e_msg = e.__str__()
        formatted_tb = ''

        # case (1): lambda function --> simply use get_source module
        if udf.__name__ == '<lambda>':
            # Lambda expressions in python consist of one line only. simply iterate code here
            formatted_tb = 'line 1, in <lambda>:\n\t' + get_source(udf)  # use reflection module
        # case (2) function defined via def
        else:
            # print out traceback (with relative line numbers!)
            formatted_tb = format_traceback(e.__traceback__.tb_next, fname)

        # return traceback and add exception type + its message
        return formatted_tb + '\n\n{}: {}'.format(etype_name, e_msg)
    return ''