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

from __future__ import unicode_literals

import os
import sys
import re
import logging
from code import InteractiveConsole
from prompt_toolkit.history import InMemoryHistory
# old version: 1.0
# from prompt_toolkit.layout.lexers import PygmentsLexer
# from prompt_toolkit.styles import style_from_pygments
# #cf. https://help.farbox.com/pygments.html for list of pygments styles available
# from pygments.styles.tango import TangoStyle
# new version: 2.0
from prompt_toolkit.lexers import PygmentsLexer
from prompt_toolkit.shortcuts import CompleteStyle
from prompt_toolkit.shortcuts import prompt as ptprompt
from prompt_toolkit.styles.pygments import style_from_pygments_cls
from pygments.lexers import Python3Lexer
from pygments.styles import get_style_by_name
from tuplex.utils.jedi_completer import JediCompleter
from tuplex.utils.source_vault import SourceVault
from types import LambdaType, FunctionType
from tuplex.utils.globs import get_globals


# this is a helper to allow for tuplex.Context syntax
# the idea is basically, we can't simply call 'import tuplex' because this would
# lead to a circular import. Yet, for user convenience, simply exposing tuplex.Context should be sufficient!
class TuplexModuleHelper:
    def __init__(self, context_cls):
        self._context_cls = context_cls

    @property
    def Context(self):
        return self._context_cls

# Interactive shell
# check https://github.com/python/cpython/blob/master/Lib/code.py for overwriting this class
class TuplexShell(InteractiveConsole):

    # use BORG design pattern to make class singleton alike
    __shared_state = {}

    def __init__(self):
        self.__dict__ = self.__shared_state

    def init(self, locals=None, filename="<console>", histfile=os.path.expanduser("~/.console_history")):

        # add dummy helper for context
        if locals is not None and 'Context' in locals.keys():
            locals['tuplex'] = TuplexModuleHelper(locals['Context'])

        self.initialized = True
        self.filename = "console-0"
        self.lineno = 0
        InteractiveConsole.__init__(self, locals, self.filename)
        self._lastLine = ''
        self.historyDict = {}

    def push(self, line):
        """Push a line to the interpreter.
        The line should not have a trailing newline; it may have
        internal newlines.  The line is appended to a buffer and the
        interpreter's runsource() method is called with the
        concatenated contents of the buffer as source.  If this
        indicates that the command was executed or invalid, the buffer
        is reset; otherwise, the command is incomplete, and the buffer
        is left as it was after the line was appended.  The return
        value is 1 if more input is required, 0 if the line was dealt
        with in some way (this is the same as runsource()).
        """
        assert self.initialized, 'must call init on TuplexShell object first'

        self.buffer.append(line)
        source = "\n".join(self.buffer)

        # save into history so extract works!
        self.historyDict[self.filename] = self.buffer.copy()

        more = self.runsource(source, self.filename)
        if not more:
            # a buffer was run, update filename with number of files!
            # use new filename with global lineno to extract code objects!
            num_lines = len(self.buffer)
            self.lineno += num_lines
            # store in global history.
            self.historyDict[self.filename] = self.buffer.copy()

            # new filename
            self.filename = 'console-{}'.format(self.lineno)
            self.resetbuffer()

        return more


    def get_lambda_source(self, f):
        # Won't this work for functions as well?

        assert self.initialized, 'must call init on TuplexShell object first'

        assert isinstance(f, LambdaType), 'object needs to be a lambda object'

        vault = SourceVault()

        # fetch all data
        f_globs = get_globals(f)
        f_filename = f.__code__.co_filename
        f_lineno = f.__code__.co_firstlineno
        f_colno = f.__code__.co_firstcolno if hasattr(f.__code__, 'co_firstcolno') else None

        # get source from history
        # Note: because firstlineno is 1-indexed, add a dummy line so everything works.
        src_info = (['dummy'] + self.historyDict[f_filename], 0)

        vault.extractAndPutAllLambdas(src_info, f_filename, f_lineno, f_colno, f_globs)
        return vault.get(f, f_filename, f_lineno, f_colno, f_globs)

    def get_function_source(self, f):

        assert self.initialized, 'must call init on TuplexShell object first'

        assert isinstance(f,
                          FunctionType) and f.__code__.co_name != '<lambda>', 'object needs to be a function (non-lambda) object'

        # fetch all data
        f_globs = get_globals(f)
        f_filename = f.__code__.co_filename
        f_lineno = f.__code__.co_firstlineno
        f_colno = f.__code__.co_firstcolno if hasattr(f.__code__, 'co_firstcolno') else None

        # retrieve func source from historyDict
        lines = self.historyDict[f_filename]

        # check whether def <name> is found in here
        source = '\n'.join(lines).strip()

        function_name = f.__code__.co_name
        regex = r"def\s*{}\(.*\)\s*:[\t ]*\n".format(function_name)
        prog = re.compile(regex)

        if not prog.search(source):
            logging.error('Could not find function "{}" in source'.format(function_name))
            return None

        return source

    # taken from Lib/code.py
    # overwritten to customize behaviour
    def interact(self, banner=None, exitmsg=None):
        """Closely emulate the interactive Python console.
        The optional banner argument specifies the banner to print
        before the first interaction; by default it prints a banner
        similar to the one printed by the real Python interpreter,
        followed by the current class name in parentheses (so as not
        to confuse this with the real interpreter -- since it's so
        close!).
        The optional exitmsg argument specifies the exit message
        printed when exiting. Pass the empty string to suppress
        printing an exit message. If exitmsg is not given or None,
        a default message is printed.
        """

        # per default use no style, no trafo because there is no easy way to determine terminal background color
        # some people love to work in a dark shell, others prefer a light one
        style = None
        style_trafo = None

        # check if env TUPLEX_COLORSCHEME is set, then pygments style may be used.
        scheme = os.environ.get('TUPLEX_COLORSCHEME', None)

        if scheme:
            # define here style for python prompt toolkit
            style = style_from_pygments_cls(get_style_by_name(scheme))

        # if dark terminal background is given, invert schem
        style_trafo = None

        history = InMemoryHistory()

        try:
            sys.ps1
        except AttributeError:
            sys.ps1 = ">>> "
        try:
            sys.ps2
        except AttributeError:
            sys.ps2 = "... "
        cprt = 'Type "help", "copyright", "credits" or "license" for more information.'
        if banner is None:
            self.write("Python %s on %s\n%s\n(%s)\n" %
                       (sys.version, sys.platform, cprt,
                        self.__class__.__name__))
        elif banner:
            self.write("%s\n" % str(banner))
        more = 0
        while 1:
            try:
                if more:
                    prompt = sys.ps2
                else:
                    prompt = sys.ps1
                try:
                    # use prompt toolkit here for more stylish input & tab completion
                    # raw python prompt
                    #line = self.raw_input(prompt)


                    # look here http://python-prompt-toolkit.readthedocs.io/en/stable/pages/asking_for_input.html#hello-world
                    # on how to style the prompt better

                    # use patch_stdout=True to output stuff above prompt
                    line = ptprompt(prompt, lexer=PygmentsLexer(Python3Lexer), style=style,
                                    style_transformation=style_trafo, history=history,
                                    completer=JediCompleter(lambda: self.locals),
                                    complete_style=CompleteStyle.READLINE_LIKE,
                                    complete_while_typing=False)

                except EOFError:
                    self.write("\n")
                    break
                else:
                    more = self.push(line)
            except KeyboardInterrupt:
                self.write("\nKeyboardInterrupt\n")
                self.resetbuffer()
                more = 0
        if exitmsg is None:
            self.write('now exiting %s...\n' % self.__class__.__name__)
        elif exitmsg != '':
            self.write('%s\n' % exitmsg)