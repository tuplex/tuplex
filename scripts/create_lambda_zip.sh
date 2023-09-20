#!/usr/bin/env bash
# (c) 2017-2023 Tuplex team
# this script creates a deployable AWS Lambda zip package using docker

set -euxo pipefail

# check which Python version is installed in /opt/lambda-python/bin/python3
PYTHON3_VERSION=$(docker run -e LD_LIBRARY_PATH=/opt/lambda-python/lib tuplex/ci /opt/lambda-python/bin/python3 --version | cut -d ' ' -f2)
PYTHON3_MAJMIN=${PYTHON3_VERSION%.*}

# check from where script is invoked
CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

echo "Executing buildwheel script located in $CWD"
pushd $CWD > /dev/null
cd .. # go to root of repo

# start code here...

LOCAL_BUILD_FOLDER=build-lambda
SRC_FOLDER=tuplex
DOCKER_IMAGE=tuplex/ci

# convert to absolute paths
get_abs_filename() {
  # $1 : relative filename
  echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}

LOCAL_BUILD_FOLDER=$(get_abs_filename $LOCAL_BUILD_FOLDER)
SRC_FOLDER=$(get_abs_filename $SRC_FOLDER)
LLVM_ROOT_PATH=/opt/llvm-16.0.6
echo "Tuplex source: $SRC_FOLDER"
echo "Building lambda in: $LOCAL_BUILD_FOLDER"
echo "LLVM folder: ${LLVM_ROOT_PATH}"
mkdir -p $LOCAL_BUILD_FOLDER

echo "starting docker (this might take a while...)"

# start docker & volume & create awslambda target with correct settings
# the python version to use for lambda is in /opt/lambda-python/bin/python3.8
# In order to kick-off the build within the docker, use the following two commands:
# export LD_LIBRARY_PATH=/opt/lambda-python/lib:$LD_LIBRARY_PATH
# cmake -DBUILD_FOR_LAMBDA=ON -DBUILD_WITH_AWS=ON -DBOOST_ROOT=/opt/boost/python3.8/ -GNinja -DPYTHON3_EXECUTABLE=/opt/lambda-python/bin/python3.8 /code/tuplex
# --> The preload is necessary as a shared version of python is used.
# just use tplxlam as target, then run custom python script to package contents up.

# only release works, b.c. of size restriction
BUILD_TYPE=Release

docker run --name lambda --rm -v $SRC_FOLDER:/code/tuplex -v $LOCAL_BUILD_FOLDER:/build tuplex/ci bash -c "export LD_LIBRARY_PATH=/opt/lambda-python/lib:/opt/lib:/opt/lib64:\$LD_LIBRARY_PATH && /opt/lambda-python/bin/python${PYTHON3_MAJMIN} -m pip install cloudpickle numpy && cd /build && cmake -DCMAKE_BUILD_TYPE=${BUILD_TYPE} -DLLVM_ROOT_DIR=${LLVM_ROOT_PATH} -DBUILD_FOR_LAMBDA=ON -DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON -DPYTHON3_EXECUTABLE=/opt/lambda-python/bin/python${PYTHON3_MAJMIN} -DBOOST_ROOT=/opt/boost/python${PYTHON3_MAJMIN}/ -GNinja /code/tuplex && cmake --build . --target tplxlam && python${PYTHON3_MAJMIN} /code/tuplex/python/zip_cc_runtime.py --input /build/dist/bin/tplxlam --runtime /build/dist/bin/tuplex_runtime.so --python /opt/lambda-python/bin/python${PYTHON3_MAJMIN} --output /build/tplxlam.zip"
DOCKER_EXIT_CODE=$?
if [ "${DOCKER_EXIT_CODE}" -eq "0" ]; then
   echo "docker command run, zipped Lambda file can be found in: ${LOCAL_BUILD_FOLDER}/tplxlam.zip"
else
   echo "build failed"
   popd > /dev/null
   exit 1
fi

# end code here...
popd > /dev/null
