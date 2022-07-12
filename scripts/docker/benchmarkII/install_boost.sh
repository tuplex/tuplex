#!/usr/bin/env bash

PYTHON_EXECUTABLE=python3.9
DEST_PATH=/opt
PYTHON_VERSION="$(basename -- $PYTHON_EXECUTABLE)"
echo "building boost for ${PYTHON_VERSION}"

mkdir -p $DEST_PATH

# fix up for boost python a link
cd /usr/local/include/ && ln -s python3.6m python3.6 && cd - || exit

# build incl. boost python
cd /usr/src || exit
wget https://boostorg.jfrog.io/artifactory/main/release/1.75.0/source/boost_1_75_0.tar.gz
tar xf boost_1_75_0.tar.gz
cd /usr/src/boost_1_75_0 || exit

./bootstrap.sh --with-python=${PYTHON_VERSION} --prefix=${DEST_PATH} --with-libraries="thread,iostreams,regex,system,filesystem,python,stacktrace,atomic,chrono,date_time"
./b2 cxxflags="-fPIC" link=static -j "$(nproc)"
./b2 cxxflags="-fPIC" link=static install
