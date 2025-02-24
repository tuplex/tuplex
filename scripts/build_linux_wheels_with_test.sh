#!/usr/bin/env bash
# (c) 2023 Tuplex team
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

# uncomment to prefer local image when building locally
# export CIBW_MANYLINUX_X86_64_IMAGE='tuplex/ci'

# check whether lambda zip was build and stored in build-lambda
TUPLEX_LAMBDA_ZIP=${TUPLEX_LAMBDA_ZIP:-build-lambda/tplxlam.zip}

echo "work dir is: $(pwd)"
if [[ -f "${TUPLEX_LAMBDA_ZIP}" ]]; then
	echo "Found lambda runner ${TUPLEX_LAMBDA_ZIP}, adding to package"
	mkdir -p tuplex/other
	cp ${TUPLEX_LAMBDA_ZIP} tuplex/other/tplxlam.zip
fi

# add to environment, e.g. TUPLEX_BUILD_TYPE=tsan to force a tsan build. Release is the default mode
export CIBW_ENVIRONMENT="TUPLEX_LAMBDA_ZIP='./tuplex/other/tplxlam.zip' CMAKE_ARGS='-DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON' LD_LIBRARY_PATH=/usr/local/lib:/opt/lib"

# Use the following line to build only python3.7-3.9 wheel
export CIBW_BUILD="cp3{9,10,11,12,13}-*"
export CIBW_ARCHS_LINUX="x86_64"

# do not build musllinux yet
export CIBW_SKIP="*-musllinux_*"

export CIBW_BUILD_VERBOSITY=3
export CIBW_PROJECT_REQUIRES_PYTHON=">=3.9"

# uncomment to increase verbosity of cibuildwheel
# export CIBW_BUILD_VERBOSITY=3

export CIBW_TEST_REQUIRES="pytest pytest-timeout numpy nbformat jupyter"
export CIBW_TEST_COMMAND="cd {project} && pytest tuplex/python/tests --timeout_method thread --timeout 300 -l -v"

cibuildwheel --platform linux .

popd > /dev/null

echo "Done!"
