#!/usr/bin/env bash
# to build the lambda executor need to embed python, therefore create full version below

export CFLAGS=-I/usr/include/openssl

# use Python 3.9 runtime
PYTHON3_VERSION=3.9.13
PYTHON3_MAJMIN=${PYTHON3_VERSION%.*}
# from https://bugs.python.org/issue36044
# change tasks, because hangs at test_faulthandler...
export PROFILE_TASK=-m test.regrtest --pgo         test_collections         test_dataclasses         test_difflib         test_embed         test_float         test_functools         test_generators         test_int         test_itertools         test_json         test_logging         test_long         test_ordered_dict         test_pickle         test_pprint         test_re         test_set         test_statistics         test_struct         test_tabnanny         test_xml_etree

set -ex && cd /tmp && wget https://www.python.org/ftp/python/${PYTHON3_VERSION}/Python-${PYTHON3_VERSION}.tgz && tar xf Python-${PYTHON3_VERSION}.tgz     && cd Python-${PYTHON3_VERSION} && ./configure --with-lto --prefix=/opt/lambda-python --enable-optimizations --enable-shared     && make -j $(( 1 * $( egrep '^processor[[:space:]]+:' /proc/cpuinfo | wc -l ) ))     && make altinstall

# install cloudpickle numpy for Lambda python
export LD_LIBRARY_PATH=/opt/lambda-python/lib:$LD_LIBRARY_PATH
/opt/lambda-python/bin/python${PYTHON3_MAJMIN} -m pip install 'cloudpickle<2.0.0' numpy tqdm