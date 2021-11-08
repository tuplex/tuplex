#!/usr/bin/env bash
# (c) 2021 Tuplex team

# this script creates a deployable AWS Lambda zip package using docker

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
echo "Tuplex source: $SRC_FOLDER"
echo "Building lambda in: $LOCAL_BUILD_FOLDER"

mkdir -p $LOCAL_BUILD_FOLDER

echo "starting docker (this might take a while...)"
# start docker & volume & create awslambda target with correct settings
# the python version to use for lambda is in /opt/lambda-python/bin/python3.8
# In order to kick-off the build within the docker, use the following two commands:
# export LD_LIBRARY_PATH=/opt/lambda-python/lib:$LD_LIBRARY_PATH
# cmake -DBUILD_FOR_LAMBDA=ON -DBUILD_WITH_AWS=ON -DBOOST_ROOT=/opt/boost/python3.8/ -GNinja -DPYTHON3_EXECUTABLE=/opt/lambda-python/bin/python3.8 /code/tuplex

# --> The preload is necessary as a shared version of python is used.

## just use tplxlam as target, then run custom python script...

docker run --name lambda --rm -v $SRC_FOLDER:/code/tuplex -v $LOCAL_BUILD_FOLDER:/build tuplex/ci bash -c "export LD_LIBRARY_PATH=/opt/lambda-python/lib:\$LD_LIBRARY_PATH && /opt/lambda-python/bin/python3.8 -m pip install cloudpickle numpy && cd /build && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_FOR_LAMBDA=ON -DBUILD_WITH_AWS=ON -DPYTHON3_EXECUTABLE=/opt/lambda-python/bin/python3.8 -DBOOST_ROOT=/opt/boost/python3.8/ -GNinja /code/tuplex && cmake --build . --target tplxlam && python3.8 /code/tuplex/python/zip_cc_runtime.py --input /build/dist/bin/tplxlam --runtime /build/dist/bin/tuplex_runtime.so --python /opt/lambda-python/bin/python3.8 --output /build/tplxlam.zip"

#docker run --name lambda --rm -v $SRC_FOLDER:/code/tuplex -v $LOCAL_BUILD_FOLDER:/build tuplex/ci bash -c "cd /build && cmake -DBUILD_FOR_LAMBDA=ON -DBUILD_WITH_AWS=ON -DPYTHON3_VERSION=3.8 -DBOOST_ROOT=/opt/boost/python3.8/ -GNinja /code/tuplex && cmake --build . --target aws-lambda-package-tplxlam"

# read-only version, fails because of managed folder in codegen/
#docker run --name lambda --rm -v $SRC_FOLDER:/code/tuplex:ro -v $LOCAL_BUILD_FOLDER:/build tuplex/ci bash -c "cd /build && cmake -DBUILD_FOR_LAMBDA=ON -DBUILD_WITH_AWS=ON -DPYTHON3_VERSION=3.8 -DBOOST_ROOT=/opt/boost/python3.8/ -GNinja /code/tuplex && cmake --build . --target aws-lambda-package-tplxlam"

echo "docker command run, zipped Lambda file can be found in: ${LOCAL_BUILD_FOLDER}/tplxlam.zip"

#
#cd build-lambda
#
## within docker...
#
## this is the command that's sufficient::::
#
#
#cmake -DPYTHON3_VERSION=3.8 -DBOOST_ROOT=/opt/boost/python3.8/ ..


# end code here...
popd > /dev/null