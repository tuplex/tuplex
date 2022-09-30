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

# change into this setup.py's dir
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

logging.basicConfig(level=logging.INFO)
logging.info('installing for {} (python {}.{})'.format(sys.executable, sys.version_info[0], sys.version_info[1]))


def tplx_package_data():
    package_data = {
        # include libs in libexec
        'tuplex.libexec' : ['*.so', '*.dylib'],
    }

    # check if thserver exists
    if os.path.isdir('tuplex/thserver'):
        logging.debug('Packaging historyserver')
        package_data['tuplex.historyserver'] = ['thserver/templates/*.html', 'thserver/static/css/*.css', 'thserver/static/css/styles/*.css',
                                 'thserver/static/img/*.*', 'thserver/static/js/*.js', 'thserver/static/js/modules/*.js',
                                 'thserver/static/js/styles/*.css']

    # package lambda as well?
    if os.path.isdir('tuplex/other'):
        logging.debug('Packaging Lambda runner')
        package_data['tuplex.other'] = ['tuplex/other/*.zip']
    return package_data

setup(
    name="Tuplex",
    version="0.3.4dev",
    packages=find_packages(),
    package_data=tplx_package_data(),
    include_package_data=True,
    # metadata for upload to PyPI
    author="Leonhard F. Spiegelberg",
    author_email="leonhard_spiegelberg@brown.edu",
    description="Tuplex is a novel big data analytics framework incorporating a Python UDF compiler together a query compiler featuring whole-stage code generation. It can be used as drop-in replacement for PySpark or Dask.",
    license="Apache 2.0",
    keywords="ETL BigData Python LLVM UDF",
    install_requires=[
        'jupyter<7.0',
        'nbconvert<7.0',
        'nbformat<7.0',
        'Werkzeug<2.2.0',
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
        'cloudpickle>=0.6.1,<2.0.0', # cloudpickle 2.x is too buggy to use yet
        'PyYAML>=3.13',
        'psutil',
        'pymongo',
        'iso8601'
    ],
    url="https://tuplex.cs.brown.edu"
    #,
    # project_urls={
    #     "Bug Tracker": "https://bugs.example.com/HelloWorld/",
    #     "Documentation": "https://docs.example.com/HelloWorld/",
    #     "Source Code": "https://code.example.com/HelloWorld/",
    # }

    # could also include long_description, download_url, classifiers, etc.
)
