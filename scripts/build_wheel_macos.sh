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

# Much of this is taken from https://github.com/matthew-brett/multibuild.
# This script uses "sudo", so you may need to type in a password a couple times.

MACPYTHON_URL=https://www.python.org/ftp/python
MACPYTHON_PY_PREFIX=/Library/Frameworks/Python.framework/Versions
DOWNLOAD_DIR=python_downloads

NODE_VERSION="14"
PY_VERSIONS=("3.6.8"
             "3.7.9"
             "3.8.10"
             "3.9.10")
NUMPY_VERSIONS=("1.14.5"
  "1.14.5"
  "1.14.5"
  "1.19.3")
PY_INSTS=("python-3.6.8-macosx10.6.pkg"
          "python-3.7.9-macosx10.9.pkg"
          "python-3.8.10-macosx10.9.pkg"
          "python-3.9.10-macosx10.9.pkg")
PY_MMS=("3.6"
        "3.7"
        "3.8"
        "3.9")

mkdir -p $DOWNLOAD_DIR
mkdir -p .whl


for ((i=0; i<${#PY_VERSIONS[@]}; ++i)); do
  PY_VERSION=${PY_VERSIONS[i]}
  PY_INST=${PY_INSTS[i]}
  PY_MM=${PY_MMS[i]}
  NUMPY_VERSION=${NUMPY_VERSIONS[i]}

  # Install Python.
  # In Buildkite, the Python packages are installed on the machine before the build has ran.
  PYTHON_EXE=$MACPYTHON_PY_PREFIX/$PY_MM/bin/python$PY_MM
  PIP_CMD="$(dirname "$PYTHON_EXE")/pip$PY_MM"

  # check if installed version exists, if not install proper python version!
  INSTALLED_PY_VERSION=""
  if [ -f $PYTHON_EXE ]; then
    echo "found python $PYTHON_EXE"
    INSTALLED_PY_VERSION=$($PYTHON_EXE --version | perl -pe 'if(($_)=/([0-9]+([.][0-9]+)+)/){$_.="\n"}')
  fi

  if [ "$INSTALLED_PY_VERSION" != "$PY_VERSION" ]; then
    echo "installed py-version ${INSTALLED_PY_VERSION} does not match desired version ${PY_VERSION}, reinstall."
    if [ -z "${BUILDKITE}" ]; then
      INST_PATH=python_downloads/$PY_INST
      curl $MACPYTHON_URL/"$PY_VERSION"/"$PY_INST" > "$INST_PATH"
      sudo installer -pkg "$INST_PATH" -target /
      #installer -pkg "$INST_PATH" -target /

      pushd /tmp
      # Install latest version of pip to avoid brownouts.
        if [ "$PY_MM" = "3.6" ]; then
          curl https://bootstrap.pypa.io/pip/3.6/get-pip.py | $PYTHON_EXE
        else
          curl https://bootstrap.pypa.io/get-pip.py | $PYTHON_EXE
        fi
      popd
    fi

  fi


  #pushd python
    # Setuptools on CentOS is too old to install arrow 0.9.0, therefore we upgrade.
    # TODO: Unpin after https://github.com/pypa/setuptools/issues/2849 is fixed.
    $PIP_CMD install --upgrade setuptools==58.4
    # Install setuptools_scm because otherwise when building the wheel for
    # Python 3.6, we see an error.
    $PIP_CMD install -q setuptools_scm==3.1.0
    # Fix the numpy version because this will be the oldest numpy version we can
    # support.
    $PIP_CMD install -q numpy=="$NUMPY_VERSION" cython==0.29.26
    # Install wheel to avoid the error "invalid command 'bdist_wheel'".
    $PIP_CMD install -q wheel

    # Add the correct Python to the path and build the wheel. This is only
    # needed so that the installation finds the cython executable.
    # build ray wheel
    #PATH=$MACPYTHON_PY_PREFIX/$PY_MM/bin:$PATH $PYTHON_EXE setup.py bdist_wheel
    # build ray-cpp wheel
    #RAY_INSTALL_CPP=1 PATH=$MACPYTHON_PY_PREFIX/$PY_MM/bin:$PATH $PYTHON_EXE setup.py bdist_wheel
    #mv dist/*.whl ../.whl/
  #popd
done

#
#
# MACPYTHON_PY_PREFIX=/Library/Frameworks/Python.framework/Versions
# PY_WHEEL_VERSIONS=("36" "37" "38")
# PY_MMS=("3.6"
#         "3.7"
#         "3.8")
#
# for ((i=0; i<${#PY_MMS[@]}; ++i)); do
#   PY_MM="${PY_MMS[i]}"
#
#   PY_WHEEL_VERSION="${PY_WHEEL_VERSIONS[i]}"
#
#   PYTHON_EXE="$MACPYTHON_PY_PREFIX/$PY_MM/bin/python$PY_MM"
#   PIP_CMD="$(dirname "$PYTHON_EXE")/pip$PY_MM"
#
#   # Find the appropriate wheel by grepping for the Python version.
#   PYTHON_WHEEL="$(printf "%s\n" "$ROOT_DIR"/../../.whl/*"$PY_WHEEL_VERSION"* | head -n 1)"
#
#   echo $PY_MM
#   echo $PY_WHEEL_VERSION
#   echo $PIP_CMD
#   echo $PYTHON_WHEEL
#
#   # # Install the wheel.
#   # "$PIP_CMD" install -q "$PYTHON_WHEEL"
#   #
#   # # Install the dependencies to run the tests.
#   # "$PIP_CMD" install -q aiohttp grpcio pytest==5.4.3 requests
#   #
#   # # Run a simple test script to make sure that the wheel works.
#   # for SCRIPT in "${TEST_SCRIPTS[@]}"; do
#   #   retry "$PYTHON_EXE" "$SCRIPT"
#   # done
# done
