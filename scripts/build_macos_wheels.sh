#!/usr/bin/env bash

# check from where script is invoked
CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

echo "Executing buildwheel script located in $CWD"
pushd $CWD > /dev/null
cd ..

# protobuf 3.20-3.21.2 is broken for MacOS, so use
# 3.19.4
# brew tap-new $USER/local-podman
# brew extract --version=3.19.4 protobuf $USER/local-podman
# brew install $USER/local-podman/protobuf@3.19.4

export CIBW_BEFORE_BUILD_MACOS="brew tap-new $USER/local; brew extract --force --version=3.19.4 protobuf $USER/local && brew install $USER/local/protobuf@3.19.4 && brew install coreutils zstd zlib libmagic llvm@9 aws-sdk-cpp pcre2 antlr4-cpp-runtime googletest gflags yaml-cpp celero wget boost"
export CIBW_ENVIRONMENT_MACOS="MACOSX_DEPLOYMENT_TARGET=10.13 CMAKE_ARGS='-DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON' "

export CIBW_BUILD="cp3{7,8,9}-*"
export CIBW_SKIP="cp3{5,6}-macosx* pp* *-musllinux_*"

export CIBW_PROJECT_REQUIRES_PYTHON=">=3.7"

cibuildwheel --platform macos

popd
