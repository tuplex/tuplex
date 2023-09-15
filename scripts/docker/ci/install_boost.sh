#!/usr/bin/env bash
#(c) 2017-2023 Tuplex team

set -euxo pipefail

# this a script to install boost for specific python version to some folder
USAGE="./install_boost.sh <PYTHON_EXECUTABLE> <PREFIX> <BOOST_VERSION>"
PYTHON_EXECUTABLE=${1:?Usage: ${USAGE}}
PREFIX=${2:?Usage: ${USAGE}}
BOOST_VERSION=${3:?Usage: ${USAGE}}

PYTHON_VERSION="$(basename -- $PYTHON_EXECUTABLE)"
echo ">>> building boost for ${PYTHON_VERSION}"
echo " -- boost will be installed to ${PREFIX}"

# fix up for boost python a link
INCLUDE_DIR=$(echo $(which $PYTHON_EXECUTABLE) | sed 's|/bin/.*||')
INCLUDE_DIR=${INCLUDE_DIR}/include
cd $INCLUDE_DIR && ln -s ${PYTHON_VERSION}m ${PYTHON_VERSION} && cd - || exit 1

WORKDIR=/tmp/tuplex-downloads

echo ">> Installing Boost version ${BOOST_VERSION} to ${PREFIX}"
mkdir -p ${WORKDIR}/boost

# create underscored version
# i.e. 1.79.0 -> 1_79_0
BOOST_UNDERSCORED_VERSION=$(echo ${BOOST_VERSION} | tr . _)

# build incl. boost python
pushd ${WORKDIR}/boost && curl -L -O https://boostorg.jfrog.io/artifactory/main/release/${BOOST_VERSION}/source/boost_${BOOST_UNDERSCORED_VERSION}.tar.gz && tar xf boost_${BOOST_UNDERSCORED_VERSION}.tar.gz && cd ${WORKDIR}/boost/boost_${BOOST_UNDERSCORED_VERSION} \
           && ./bootstrap.sh --with-python=${PYTHON_EXECUTABLE} --prefix=${PREFIX} --with-libraries="thread,iostreams,regex,system,filesystem,python,stacktrace,atomic,chrono,date_time" \
            && ./b2 cxxflags="-fPIC" link=static -j "$(nproc)" \
            && ./b2 cxxflags="-fPIC" link=static install && sed -i 's/#if PTHREAD_STACK_MIN > 0/#ifdef PTHREAD_STACK_MIN/g' ${PREFIX}/include/boost/thread/pthread/thread_data.hpp

rm -rf ${WORKDIR}/boost
