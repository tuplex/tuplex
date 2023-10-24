#!/usr/bin/env bash
# (c) 2017-2023 Tuplex team
# builds x86_64 (and arm64) wheels

# uncomment for debug
#set -euxo pipefail
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

xcode_version_str=$(pkgutil --pkg-info=com.apple.pkg.CLTools_Executables | grep version)
echo "-- Detected Xcode ${xcode_version_str}"

# if no param is given, use defaults to build all
if [ "${arch}" = "arm64" ]; then
  # build Python 3.9 - 3.11
#  cp38-macosx_arm64
  CIBW_BUILD=${CIBW_BUILD-"cp3{9,10,11}-macosx_arm64"}
else
  # build Python 3.8 - 3.11
  CIBW_BUILD=${CIBW_BUILD-"cp3{8,9,10,11}-macosx_x86_64}"}
fi

echo "-- Building wheels for ${CIBW_BUILD}"

# if macOS is 10.x -> use this as minimum
MINIMUM_TARGET="10.13"

MACOS_VERSION=$(sw_vers -productVersion)
echo "-- processing on MacOS ${MACOS_VERSION}"
function version { echo "$@" | awk -F. '{ printf("%d%03d%03d%03d\n", $1,$2,$3,$4); }'; }

MACOS_VERSION_MAJOR=`echo $MACOS_VERSION | cut -d . -f1`

if [ "$MACOS_VERSION_MAJOR" -ge 11 ]; then
    echo "-- Newer MacOS detected (>=11.0), using more recent base target."
    echo "-- Using minimum target ${MACOS_VERSION_MAJOR}.0"
    MINIMUM_TARGET="${MACOS_VERSION_MAJOR}.0"
else
    # keep as is
    echo "-- defaulting build to use as minimum target ${MINIMUM_TARGET}"
fi

pushd $CWD > /dev/null
cd ..

# protobuf 3.20-3.21.2 is broken for MacOS, so use
# 3.19.4
# brew tap-new $USER/local-podman
# brew extract --version=3.19.4 protobuf $USER/local-podman
# brew install $USER/local-podman/protobuf@3.19.4
# i.e., prepend to statemtnt the following: brew tap-new $USER/local; brew extract --force --version=3.19.4 protobuf $USER/local && brew install $USER/local/protobuf@3.19.4 &&
export CIBW_BEFORE_BUILD_MACOS="brew install protobuf coreutils zstd zlib libmagic llvm@16 aws-sdk-cpp pcre2 antlr4-cpp-runtime googletest gflags yaml-cpp celero wget boost ninja snappy"
export CIBW_ENVIRONMENT_MACOS="MACOSX_DEPLOYMENT_TARGET=${MINIMUM_TARGET} CMAKE_ARGS='-DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON' "

export CIBW_BUILD="${CIBW_BUILD}"
export CIBW_PROJECT_REQUIRES_PYTHON=">=3.8"

export CIBW_BUILD_VERBOSITY=3

cibuildwheel --platform macos

popd
