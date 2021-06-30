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

def classToExceptionCode(cls):
    """
    return C++ enum exception code for class
    Args:
        cls: an exception class, must be inheriting from Exception

    Returns: integer encoding exception type

    """

    lookup = {BaseException : 100,
              Exception : 101,
              ArithmeticError : 102,
              BufferError : 103,
              LookupError : 104,
              AssertionError : 105,
              AttributeError : 106,
              EOFError : 107,
              GeneratorExit : 108,
              ImportError : 109,
              ModuleNotFoundError : 110,
              IndexError : 111,
              KeyError : 112,
              KeyboardInterrupt : 113,
              MemoryError : 114,
              NameError : 115,
              NotImplementedError : 116,
              OSError : 117,
              OverflowError : 118,
              RecursionError : 119,
              ReferenceError : 120,
              RuntimeError : 121,
              StopIteration : 122,
              StopAsyncIteration : 123,
              SyntaxError : 124,
              IndentationError : 125,
              TabError : 126,
              SystemError : 127,
              SystemExit : 128,
              TypeError : 129,
              UnboundLocalError : 130,
              UnicodeError : 131,
              UnicodeEncodeError : 132,
              UnicodeDecodeError : 133,
              UnicodeTranslateError : 134,
              ValueError : 135,
              ZeroDivisionError : 136,
              EnvironmentError : 137,
              IOError : 138,
              BlockingIOError : 139,
              ChildProcessError : 140,
              ConnectionError : 141,
              BrokenPipeError : 142,
              ConnectionAbortedError : 143,
              ConnectionRefusedError : 144,
              FileExistsError : 145,
              FileNotFoundError : 146,
              InterruptedError : 147,
              IsADirectoryError : 148,
              NotADirectoryError : 149,
              PermissionError : 150,
              ProcessLookupError : 151,
              TimeoutError : 152
              }

    try:
        return lookup[cls]
    except:
        return