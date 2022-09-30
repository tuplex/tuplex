#!/usr/bin/env bash
# (c) 2021 Tuplex team
# this script invokes the cibuildwheel process with necessary env variables to build the wheel for linux/docker
# builds wheels for python 3.7 - 3.9

# check from where script is invoked
CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

echo "Executing buildwheel script located in $CWD"
pushd $CWD > /dev/null
cd ..

# delete dir if exists
rm -rf wheelhouse
# delete in tree build files
rm -rf tuplex/python/tuplex/libexec/tuplex*.so


# CIBUILDWHEEL CONFIGURATION
export CIBUILDWHEEL=1
export TUPLEX_BUILD_ALL=0
export CIBW_ARCHS_LINUX=x86_64
export CIBW_MANYLINUX_X86_64_IMAGE='registry-1.docker.io/tuplex/ci:latest'


# check whether lambda zip was build and stored in build-lambda
TUPLEX_LAMBDA_ZIP=${TUPLEX_LAMBDA_ZIP:-build-lambda/tplxlam.zip}


echo "work dir is: $(pwd)"
if [[ -f "${TUPLEX_LAMBDA_ZIP}" ]]; then
	echo "Found lambda runner ${TUPLEX_LAMBDA_ZIP}, adding to package"
	mkdir -p tuplex/other
	cp ${TUPLEX_LAMBDA_ZIP} tuplex/other/tplxlam.zip
fi

export CIBW_ENVIRONMENT="TUPLEX_LAMBDA_ZIP='./tuplex/other/tplxlam.zip' CMAKE_ARGS='-DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON' LD_LIBRARY_PATH=/usr/local/lib:/opt/lib"

# Use the following line to build only python3.7-3.9 wheel
export CIBW_BUILD="cp3{7,8,9}-*"
export CIBW_ARCHS_LINUX="x86_64"

# do not build musllinux yet
export CIBW_SKIP="*-musllinux_*"

# to test the others from 3.7-3.9, use these two lines:
#export CIBW_BUILD="cp3{7,8,9}-*"
#export CIBW_SKIP="cp3{5,6,7,8}-macosx* pp*"

export CIBW_BUILD_VERBOSITY=3
export CIBW_PROJECT_REQUIRES_PYTHON=">=3.7"

cibuildwheel --platform linux .

popd > /dev/null

echo "Done!"
