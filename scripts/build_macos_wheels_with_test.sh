#!/usr/bin/env bash
# (c) 2017-2023 Tuplex team
# builds x86_64 (and arm64) wheels

# add -x option for verbose output
set -euo pipefail

function fail {
    printf '%s\n' "$1" >&2
    exit "${2-1}"
}

function detect_instruction_set() {
  arch="$(uname -m)"  # -i is only linux, -m is linux and apple
  if [[ "$arch" = x86_64* ]]; then
    if [[ "$(uname -a)" = *ARM64* ]]; then
      echo 'arm64'
    else
      echo 'x86_64'
    fi
  elif [[ "$arch" = i*86 ]]; then
    echo 'x86_32'
  elif [[ "$arch" = arm* ]]; then
    echo $arch
  elif test "$arch" = aarch64; then
    echo 'arm64'
  else
    exit 1
	fi
}

# check from where script is invoked
CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
echo " || Tuplex macOS wheel builder || "
echo "-- Executing buildwheel script located in $CWD"

# check platform is darwin
if [ ! "$(uname -s)" = "Darwin" ]; then
  fail "Error: Need to run script under macOS"
fi

# check which tags are supported
arch=$(detect_instruction_set)
echo "-- Detected arch ${arch}"

# try to extract version of compiler first via command-line tools or xcode
# either needs to be installed.
xcode_version_str=$(pkgutil --pkg-info=com.apple.pkg.CLTools_Executables 2>/dev/null | grep version || pkgutil --pkg-info=com.apple.pkg.Xcode | grep version)
echo "-- Detected Xcode ${xcode_version_str}"

# if no param is given, use defaults to build all
if [ "${arch}" = "arm64" ]; then
  # build Python 3.9 - 3.11
  CIBW_BUILD=${CIBW_BUILD-"cp3{9,10,11}-macosx_arm64"}
else
  # build Python 3.8 - 3.11
  CIBW_BUILD=${CIBW_BUILD-"cp3{8,9,10,11}-macosx_x86_64"}
fi

echo "-- Building wheels for ${CIBW_BUILD}"

MACOS_VERSION=$(sw_vers -productVersion)
echo "-- Processing on MacOS ${MACOS_VERSION}"
function version { echo "$@" | awk -F. '{ printf("%d%03d%03d%03d\n", $1,$2,$3,$4); }'; }

MACOS_VERSION_MAJOR=`echo $MACOS_VERSION | cut -d . -f1`

if [ "$MACOS_VERSION_MAJOR" -ge 11 ]; then
    echo "-- Newer MacOS detected (>=11.0), using more recent base target."
    echo "-- Using minimum target ${MACOS_VERSION_MAJOR}.0"
    MINIMUM_TARGET="${MACOS_VERSION_MAJOR}.0"
else
    # keep as is
    echo "-- Defaulting build to use as minimum target ${MINIMUM_TARGET}"
fi

pushd $CWD > /dev/null
cd ..

# fix because of Python
MINIMUM_TARGET=11.0

# Note: 3.8 only supports tags up to 10.16
MINIMUM_TARGET=10.13

# Note: protobuf 3.20 - 3.21.2 is broken for MacOS, do not use those versions
export CIBW_BEFORE_BUILD_MACOS="brew install protobuf coreutils zstd zlib libmagic llvm@16 aws-sdk-cpp pcre2 antlr4-cpp-runtime googletest gflags yaml-cpp celero wget boost ninja snappy libdwarf libelf"


# Note: orc build breaks wheel right now...
#export CIBW_ENVIRONMENT_MACOS="MACOSX_DEPLOYMENT_TARGET=${MINIMUM_TARGET} CMAKE_ARGS='-DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON' TUPLEX_BUILD_TYPE=RelWithDebInfo"
export CIBW_ENVIRONMENT_MACOS="MACOSX_DEPLOYMENT_TARGET=${MINIMUM_TARGET} CMAKE_ARGS='-DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON' TUPLEX_BUILD_TYPE=Debug"

export CIBW_BUILD="${CIBW_BUILD}"
export CIBW_PROJECT_REQUIRES_PYTHON=">=3.8"

# uncomment to increase verbosity of cibuildwheel
export CIBW_BUILD_VERBOSITY=3

# uncomment and set to specific identifier
#export CIBW_BUILD="cp39-macosx_x86_64"

export CIBW_TEST_REQUIRES="pytest pytest-timeout numpy nbformat jupyter"
export CIBW_TEST_COMMAND="cd {project} && pytest tuplex/python/tests/test_exceptions.py --timeout_method thread --timeout 180 -l -v -s"

#export CIBW_TEST_REQUIRES="pytest pytest-timeout pytest-xdist numpy nbformat jupyter"
#export CIBW_TEST_COMMAND="cd {project} && pytest tuplex/python/tests/test_exceptions.py::TestExceptions -l -v"
#export CIBW_TEST_COMMAND="cd {project} && pytest tuplex/python/tests -n auto --timeout 600 -l -v"

cibuildwheel --platform macos

popd
