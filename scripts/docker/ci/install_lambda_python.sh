#!/usr/bin/env bash
# (c) 2017 - 2023
# to build the lambda executor need to embed python, therefore create full version below

set -euxo pipefail

export CFLAGS=-I/usr/include/openssl

CPU_COUNT=$(( 1 * $( egrep '^processor[[:space:]]+:' /proc/cpuinfo | wc -l ) ))

# use the version provided as argument
USAGE="./install_lambda_python.sh <PYTHON_VERSION>"
PYTHON3_VERSION=${1:?Usage: ${USAGE}}
PYTHON3_MAJMIN=${PYTHON3_VERSION%.*}

echo ">>> Building Python for AWS Lambda runner with version ${PYTHON3_VERSION}"

# from https://bugs.python.org/issue36044
# change tasks, because hangs at test_faulthandler...
export PROFILE_TASK="-m test.regrtest --pgo         test_collections         test_dataclasses         test_difflib         test_embed         test_float         test_functools         test_generators         test_int         test_itertools         test_json         test_logging         test_long         test_ordered_dict         test_pickle         test_pprint         test_re         test_set         test_statistics         test_struct         test_tabnanny         test_xml_etree"

cd /tmp && wget https://www.python.org/ftp/python/${PYTHON3_VERSION}/Python-${PYTHON3_VERSION}.tgz && \
        tar xf Python-${PYTHON3_VERSION}.tgz     && \
        cd Python-${PYTHON3_VERSION} && \
        ./configure ./configure --with-openssl=/usr/local --with-ssl --with-lto --prefix=/opt/lambda-python --enable-optimizations --enable-shared     && \
         make -j ${CPU_COUNT}     && make altinstall

export LD_LIBRARY_PATH=/opt/lambda-python/lib:$LD_LIBRARY_PATH

# install cloudpickle numpy pandas for Lambda python
declare -A PYTHON_DEPENDENCIES=(["3.8"]="cloudpickle<2.0 cython numpy pandas" ["3.9"]="cloudpickle<2.0 numpy pandas" ["3.10"]="cloudpickle>2.0 numpy pandas" ["3.11"]="cloudpickle>2.0 numpy pandas")
PYTHON_REQUIREMENTS=$(echo "${PYTHON_DEPENDENCIES[$PYTHON3_MAJMIN]}")
/opt/lambda-python/bin/python${PYTHON3_MAJMIN} -m pip install ${PYTHON_REQUIREMENTS} tqdm

# remove downloaded Python files from /tmp
rm -rf /tmp/Python*