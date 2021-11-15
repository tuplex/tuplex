#!/usr/bin/env bash
# this script invokes the cibuildwheel process with necessary env variables to build the wheel for linux/docker

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

export CIBW_ENVIRONMENT="TUPLEX_LAMBDA_ZIP='./tuplex/other/tplxlam.zip' LD_LIBRARY_PATH=/usr/local/lib:/opt/lib"

# Use the following line to build only python3.9 wheel
export CIBW_BUILD="cp39-*"

# For Google Colab compatible wheel, use the following:
export CIBW_BUILD="cp37-*"
export CIBW_ARCHS_LINUX="x86_64"

# do not build musllinux yet
export CIBW_SKIP="*-musllinux_*"

# to test the others from 3.7-3.9, use these two lines:
#export CIBW_BUILD="cp3{7,8,9}-*"
#export CIBW_SKIP="cp3{5,6,7,8}-macosx* pp*"

export CIBW_BUILD_VERBOSITY=3
export CIBW_PROJECT_REQUIRES_PYTHON=">=3.7"

export CIBW_REPAIR_WHEEL_COMMAND_LINUX="LD_LIBRARY_PATH=/opt/lib:/usr/local/lib:usr/lib auditwheel repair --lib-sdir . -w {dest_dir} {wheel}"

cibuildwheel --platform linux .

popd > /dev/null

echo "Done!"
