#!/usr/bin/env bash
#(c) 2017-2023 Tuplex team

# this a script to install boost for specific python version to some folder
PYTHON_EXECUTABLE=$1
PREFIX=$2
PYTHON_VERSION="$(basename -- $PYTHON_EXECUTABLE)"
echo ">>> building boost for ${PYTHON_VERSION}"
echo " -- boost will be installed to ${PREFIX}"

mkdir -p $DEST_PATH

# fix up for boost python a link
INCLUDE_DIR=$(echo $PYTHON_EXECUTABLE | sed 's|/bin/.*||')
INCLUDE_DIR=${INCLUDE_DIR}/include
cd $INCLUDE_DIR && ln -s ${PYTHON_VERSION}m ${PYTHON_VERSION} && cd - || exit 1

    
mkdir -p ${WORKDIR}/boost

# build incl. boost python
pushd ${WORKDIR}/boost && wget https://boostorg.jfrog.io/artifactory/main/release/1.79.0/source/boost_1_79_0.tar.gz && tar xf boost_1_79_0.tar.gz && cd ${WORKDIR}/boost/boost_1_79_0 \
           && ./bootstrap.sh --with-python=${PYTHON_EXECUTABLE} --prefix=${PREFIX} --with-libraries="thread,iostreams,regex,system,filesystem,python,stacktrace,atomic,chrono,date_time" \
            && ./b2 cxxflags="-fPIC" link=static -j "$(nproc)" \
            && ./b2 cxxflags="-fPIC" link=static install && sed -i 's/#if PTHREAD_STACK_MIN > 0/#ifdef PTHREAD_STACK_MIN/g' ${PREFIX}/include/boost/thread/pthread/thread_data.hpp