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

import os
import sys

from tuplex.utils.common import is_in_interactive_mode, in_jupyter_notebook, in_google_colab
try:
    from tuplex.utils.version import __version__
except:
    __version__ = 'dev'

def TuplexBanner():
    banner = '''Welcome to\n
  _____            _
 |_   _|   _ _ __ | | _____  __
   | || | | | '_ \| |/ _ \ \/ /
   | || |_| | |_) | |  __/>  <
   |_| \__,_| .__/|_|\___/_/\_\\ {}
            |_|
    '''.format(__version__)
    banner += '\nusing Python {} on {}'.format(sys.version, sys.platform)
    return banner


# if the module is imported in interactive mode, overwrite shell with own shell
# else, provide code-closure functionality through readline module

if is_in_interactive_mode() and not in_jupyter_notebook() and not in_google_colab():
    from tuplex.utils.interactive_shell import TuplexShell
    os.system('clear')

    from tuplex.context import Context
    _locals = locals()
    _locals = {key: _locals[key] for key in _locals if key in ['Context']}

    shell = TuplexShell()
    shell.init(locals=_locals)
    shell.interact(banner=TuplexBanner() + '\n Interactive Shell mode')
else:
    print(TuplexBanner())