#!/usr/bin/env bash

PYTHON_VERSION="3.9.14"
URL="https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz"

# specify here with what flags to build the version (cf. https://pythonextensionpatterns.readthedocs.io/en/latest/debugging/debug_in_ide.html)
# and https://pythonextensionpatterns.readthedocs.io/en/latest/debugging/debug_python.html
DEBUG_OPTIONS="--with-pydebug --without-pymalloc --with-valgrind"
PREFIX="$HOME/.local/python${PYTHON_VERSION}-dbg"

[ -d .cache ] && rm -rf .cache
mkdir -p .cache

# save current working dir
CWD=$PWD

cd .cache

# download python
echo ">>> downloading python ${PYTHON_VERSION}"
wget $URL
echo ">>> extracting python"
tar xf Python-${PYTHON_VERSION}.tar.xz
cd Python-${PYTHON_VERSION}

mkdir -p debug && cd debug && ../configure --prefix=${PREFIX} ${DEBUG_OPTIONS} && make -j$(nproc) && make test


cd $CWD

