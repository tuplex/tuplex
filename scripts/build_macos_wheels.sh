#!/usr/bin/env bash

# check from where script is invoked
CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

echo "Executing buildwheel script located in $CWD"
pushd $CWD > /dev/null
cd ..

export CIBW_BEFORE_BUILD_MACOS="brew install coreutils protobuf zstd zlib libmagic llvm@9 aws-sdk-cpp pcre2 antlr4-cpp-runtime googletest gflags yaml-cpp celero wget boost"
export CIBW_ENVIRONMENT_MACOS=" CMAKE_ARGS='-DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON' "

export CIBW_BUILD="cp3{7,8,9}-*"
export CIBW_SKIP="cp3{5,6}-macosx* pp* *-musllinux_*"

export CIBW_PROJECT_REQUIRES_PYTHON=">=3.7"

cibuildwheel --platform macos

popd
