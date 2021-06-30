#!/usr/bin/env bash

# tests using fallback solution require cloudpickle, so install it
pip3 install cloudpickle numpy

cd /code &&
mkdir -p build &&
cd build &&
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTS=ON .. &&
make -j 4

# use this, to also show stdout/stderr output while running tests
#ctest --verbose

# install other python dependencies
pip3 install astor PyYAML jupyter nbformat

# run cmake tests
ctest --output-on-failure

# run python tests
cd dist/python &&
python3 setup.py develop &&
pytest
