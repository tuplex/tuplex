#!/usr/bin/env bash
# this a script to install boost for specific python version to some folder
PYTHON_EXECUTABLE=$1
DEST_PATH=$2
PYTHON_VERSION="$(basename -- $PYTHON_EXECUTABLE)"
echo "building boost for ${PYTHON_VERSION}"

mkdir -p $DEST_PATH

# fix up for boost python a link
INCLUDE_DIR=$(echo $PYTHON_EXECUTABLE | sed 's|/bin/.*||')
INCLUDE_DIR=${INCLUDE_DIR}/include
cd $INCLUDE_DIR && ln -s ${PYTHON_VERSION}m ${PYTHON_VERSION} && cd - || exit


# build incl. boost python
cd /usr/src || exit
wget https://boostorg.jfrog.io/artifactory/main/release/1.75.0/source/boost_1_75_0.tar.gz
tar xf boost_1_75_0.tar.gz
cd /usr/src/boost_1_75_0 || exit

./bootstrap.sh --with-python=${PYTHON_VERSION} --prefix=${DEST_PATH} --with-libraries="thread,iostreams,regex,system,filesystem,python,stacktrace,atomic,chrono,date_time"
./b2 cxxflags="-fPIC" link=static -j "$(nproc)"
./b2 cxxflags="-fPIC" link=static install
