#!/usr/bin/env bash
# this script invokes the cibuildwheel process with necessary env variables to build the wheel for linux/docker

cd ..
# delete dir if exists
rm -rf wheelhouse
# delete in tree build files
rm -rf tuplex/python/tuplex/libexec/tuplex*.so


# CIBUILDWHEEL CONFIGURATION
export CIBUILDWHEEL=1
export CIBW_ARCHS_LINUX=native
export CIBW_MANYLINUX_X86_64_IMAGE='registry-1.docker.io/tuplex/ci:latest'

# Use the following line to build only python3.9 wheel
export CIBW_BUILD="cp39-*"

# to test the others from 3.7-3.9, use these two lines:
#export CIBW_BUILD="cp3{7,8,9}-*"
#export CIBW_SKIP="cp3{5,6,7,8}-macosx* pp*"

export CIBW_BUILD_VERBOSITY=3
export CIBW_PROJECT_REQUIRES_PYTHON=">=3.7"
cibuildwheel --platform linux .

cd scripts
