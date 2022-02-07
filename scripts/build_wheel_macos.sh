#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

platform=""
case "${OSTYPE}" in
  linux*) platform="linux";;
  darwin*) platform="macosx";;
  msys*) platform="windows";;
  *) echo "Unrecognized platform."; exit 1;;
esac

# select here py-versions to build for
MACPYTHON_PY_PREFIX=/Library/Frameworks/Python.framework/Versions
PY_VERSIONS=("3.6.8"
             "3.7.9"
             "3.8.10"
             "3.9.10")
NUMPY_VERSIONS=("1.14.5"
  "1.14.5"
  "1.14.5"
  "1.19.3")
PY_MMS=("3.6"
        "3.7"
        "3.8"
        "3.9")

mkdir -p wheelhouse

for ((i=0; i<${#PY_VERSIONS[@]}; ++i)); do
  PY_VERSION=${PY_VERSIONS[i]}
  PY_INST=${PY_INSTS[i]}
  PY_MM=${PY_MMS[i]}
  NUMPY_VERSION=${NUMPY_VERSIONS[i]}

  # Install Python.
  # In Buildkite, the Python packages are installed on the machine before the build has ran.
  PYTHON_EXE=$MACPYTHON_PY_PREFIX/$PY_MM/bin/python$PY_MM
  PIP_CMD="$(dirname "$PYTHON_EXE")/pip$PY_MM"

  # check if installed version exists, if not fail
  INSTALLED_PY_VERSION=""
  if [ ! -f $PYTHON_EXE ]; then
    echo "did not find python $PYTHON_EXE, please run setup-macos.sh first"
    INSTALLED_PY_VERSION=$($PYTHON_EXE --version | perl -pe 'if(($_)=/([0-9]+([.][0-9]+)+)/){$_.="\n"}')
  fi

  # go to root dir where root setup.py is for tuplex
  pushd $CWD/.. && \
  # Add the correct Python to the path and build the wheel.
  PATH=$MACPYTHON_PY_PREFIX/$PY_MM/bin:$PATH CMAKE_ARGS="-DBoost_USE_STATIC_LIBS=ON" $PYTHON_EXE setup.py bdist_wheel && \
  popd

done
