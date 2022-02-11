#!/usr/bin/env bash

set -ex
#set -euo pipefail

# cur dir
CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
TUPLEX_ROOT_DIR=$(realpath $CWD/..)
WHEEL_OUTPUT_DIR=$TUPLEX_ROOT_DIR/wheelhouse
# create wheelhouse dir
mkdir -p $WHEEL_OUTPUT_DIR


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
if [ $platform = 'darwin' ]; then

  # !!! make sure llvm from homebrew is NOT on path when running delocate-wheel !!!
  echo ">>> Building wheel"

  # on github actions for whatever reason zstd is installed in a wrong dir, this is a fix
  export LIBRARY_PATH=$LIBRARY_PATH:$(brew --prefix zstd)/lib/
  # target catalina 10.15?
  # could also use 10.9 to be even more restrictive
  # for now, let's be restrictive
  export MACOSX_DEPLOYMENT_TARGET=10.9
  # build wheel on mac os, add -DCMAKE_VERBOSE_MAKEFILE:BOOL=ON to print out cmake verbosely
  # CMAKE_ARGS="-DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON -DBoost_USE_STATIC_LIBS=ON"
  CMAKE_ARGS="-DBUILD_WITH_AWS=ON -DBUILD_WITH_ORC=ON -DBoost_USE_STATIC_LIBS=ON" python3 setup.py bdist_wheel

  # fix wheel using delocate
  echo ">>> Fixing wheel using delocate"
  for file in `ls ./dist/*macosx*.whl`; do
    echo " - delocating $file"

    # make sure to remove brewed llvm from path!
    # /usr/local/opt/llvm/bin
    python3 -m pip install delocate
    REPAIR_WHEEL=$(which delocate-wheel)
    echo "- found repair command @ ${REPAIR_WHEEL}"

    PATH==$(echo $PATH | sed -e 's|:[a-zA-z/]*/usr/local/opt/llvm/bin||g') $(REPAIR_WHEEL) -w $WHEEL_OUTPUT_DIR/ $file
  done
fi

echo "Done, please find fat wheels in ${WHEEL_OUTPUT_DIR}."
