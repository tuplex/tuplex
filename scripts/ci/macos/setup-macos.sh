#!/usr/bin/env bash
# use brew to setup everything

ORIGINAL_WD=$PWD

CWD="$(cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

# Cause the script to exit if a single command fails.
set -e

# Show explicitly which commands are currently running.
set -x

# this should setup python3.9
brew install python3
brew upgrade python3
brew link --force --overwrite python3

# boost and boost python have to be installed separately
brew install coreutils protobuf zstd zlib libmagic llvm@9 aws-sdk-cpp pcre2 antlr4-cpp-runtime googletest gflags yaml-cpp celero wget

# install boost and different python versions
MACPYTHON_URL=https://www.python.org/ftp/python
MACPYTHON_PY_PREFIX=/Library/Frameworks/Python.framework/Versions
DOWNLOAD_DIR=python_downloads

PY_VERSIONS=("3.6.8"
             "3.7.9"
             "3.8.10"
             "3.9.10")
NUMPY_VERSIONS=("1.14.5"
  "1.14.5"
  "1.14.5"
  "1.19.3")
PY_INSTS=("python-3.6.8-macosx10.9.pkg"
          "python-3.7.9-macosx10.9.pkg"
          "python-3.8.10-macosx10.9.pkg"
          "python-3.9.10-macosx10.9.pkg")
PY_MMS=("3.6"
        "3.7"
        "3.8"
        "3.9")

# install different python versions
mkdir -p $DOWNLOAD_DIR
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
  $PIP_CMD install -q wheel cloudpickle
done

# install boost python for this script
cd $CWD
sudo mkdir -p /opt/boost
sudo bash ./install_boost_macos.sh /opt/boost

cd $ORIGINAL_WD
