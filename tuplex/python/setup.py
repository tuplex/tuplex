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

from setuptools import setup, find_packages

# this here needs to be fixed...
# i.e. runtime + the .so module need to be copied...

import os
import sys
import logging

logging.basicConfig(level=logging.INFO)
logging.info('installing for {} (python {}.{})'.format(sys.executable, sys.version_info[0], sys.version_info[1]))

# files to copy for install
files = [os.path.join(dp, f) for dp, dn, fn in os.walk(os.path.expanduser("tuplex")) for f in fn]

# remove __pycache__ files
files = list(filter(lambda x: '__pycache__' not in x and not x.endswith('.pyc'), files))

setup(
    name="Tuplex",
    version="0.3.7",
    packages=find_packages(),
    package_data={
      # include libs in libexec
    'tuplex.libexec' : ['*.so', '*.dylib']
    },
    # metadata for upload to PyPI
    author="Leonhard F. Spiegelberg",
    author_email="leonhard_spiegelberg@brown.edu",
    description="Tuplex is a novel big data analytics framework incorporating a Python UDF compiler together a query compiler featuring whole-stage code generation. It can be used as drop-in replacement for PySpark or Dask.",
    license="Apache 2.0",
    keywords="ETL BigData Python LLVM UDF",
    install_requires=[
        'nbconvert<7.0',
        'jupyter<7.0',
        'nbformat<7.0',
        'Werkzeug',
        'attrs>=19.2.0',
        'dill>=0.2.7.1',
        'pluggy>=0.6.0, <1.0.0',
        'py>=1.5.2',
        'pygments>=2.4.1',
        'pytest>=5.3.2',
        'six>=1.11.0',
        'wcwidth>=0.1.7',
        'astor',
        'prompt_toolkit>=2.0.7',
        'jedi>=0.13.2',
        'cloudpickle>=0.6.1,<2.0.0',
        'PyYAML>=3.13',
        'psutil',
        'pymongo',
        'iso8601'
    ],
    url="https://tuplex.cs.brown.edu"
)
