#!/usr/bin/env bash
# (c) 2021 Tuplex team

# this script creates a deployable AWS Lambda zip package using docker

# check from where script is invoked
CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

echo "Executing buildwheel script located in $CWD"
pushd $CWD > /dev/null
cd .. # go to root of repo

# start code here...

mkdir build-lambda
cd build-lambda

# within docker...
cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_FOR_LAMBDA=ON -DPYTHON3_VERSION=3.8 -DBOOST_ROOT=/opt/boost/python3.8/ ..

cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_FOR_LAMBDA=ON -DPython3_INCLUDE_DIRS=/opt/python/cp38-cp38/lib/python3.8/ -DPython3_INCLUDE_DIRS=/opt/python/cp38-cp38/include/python3.8/ -DPYTHON3_VERSION=3.8 -DPYTHON_EXECUTABLE=python3.8 -DBOOST_ROOT=/opt/boost/python3.8/ ..

cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_FOR_LAMBDA=ON --DPython3_ROOT_DIR=/opt/_internal/cpython-3.8.12/ DPython3_INCLUDE_DIRS=/opt/_internal/cpython-3.8.12/lib/python3.8 -DPython3_INCLUDE_DIRS=/opt/_internal/cpython-3.8.12/include/python3.8 -DBOOST_ROOT=/opt/boost/python3.8/ ..



cmake -DCMAKE_BUILD_TYPE=Release -DPYTHON3_VERSION=3.8 -DPYTHON_EXECUTABLE=/opt/python/cp38-cp38/bin/python3 -DBOOST_ROOT=/opt/boost/python3.8/ ..


# end code here...
popd > /dev/null