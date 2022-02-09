#!/usr/bin/env bash

#set -x
#set -euo pipefail

# detect platform & display message via echo
achitecture="${HOSTTYPE}"
platform="unknown"
case "${OSTYPE}" in
  msys)
    echo "Detected Windows. Tuplex currently has no windows support, consider using WSL (https://docs.microsoft.com/en-us/windows/wsl/install)"
    platform="windows"
    exit 1
    ;;
  darwin*)
    echo "Building on MacOS"
    platform="darwin"
    ;;
  linux*)
    echo "Building on Linux (or WSL)."
    platform="linux"
    ;;
  *)
    echo "Unrecognized OS, no support."
    exit 1
esac



# MacOS wheel building

# !!! make sure llvm from homebrew is NOT on path when running delocate-wheel !!!

# build wheel on mac os
CMAKE_ARGS="-DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON -DBoost_USE_STATIC_LIBS=ON -DCMAKE_VERBOSE_MAKEFILE:BOOL=ON" python3 setup.py bdist_wheel
