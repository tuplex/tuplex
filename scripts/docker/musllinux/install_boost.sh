#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs Boost dependencies required to build tuplex on Alpine Linux.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
PYTHON_EXECUTABLE=${PYTHON_EXECUTABLE:-python3}
BOOST_VERSION=${BOOST_VERSION:-1.88.0}
CC=${CC:-gcc}
CXX=${CXX:-g++}

# Get Python version for Boost installation path
PYTHON_VERSION=$(${PYTHON_EXECUTABLE} -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
BOOST_INSTALL_DIR="${PREFIX}/boost/python${PYTHON_VERSION}"

# Get CPU count for parallel builds
CPU_COUNT=$(( 1 * $( grep '^processor[[:space:]]*:' /proc/cpuinfo | wc -l ) ))

# Start script.
set -euxo pipefail

# need to run this with root privileges
if [ "$(id -u)" -ne 0 ]; then
  echo "Please run this script with root privileges"
  exit 1
fi

echo ">> Installing Boost ${BOOST_VERSION}"

# Create download directory
DOWNLOAD_DIR=${WORKDIR}/tuplex-downloads
mkdir -p "${DOWNLOAD_DIR}/boost"

# create underscored version, e.g. 1.79.0 -> 1_79_0
boost_underscored_version=$(echo "${BOOST_VERSION}" | tr . _)

# Download and build Boost including boost python
cd "${DOWNLOAD_DIR}/boost"
curl -L -O "https://github.com/boostorg/boost/releases/download/boost-${BOOST_VERSION}/boost-${BOOST_VERSION}-b2-nodocs.tar.gz"
tar xf "boost-${BOOST_VERSION}-b2-nodocs.tar.gz"
cd "${DOWNLOAD_DIR}/boost/boost-${BOOST_VERSION}"
./bootstrap.sh --with-python="${PYTHON_EXECUTABLE}" --prefix="${BOOST_INSTALL_DIR}" --with-libraries="thread,iostreams,regex,system,filesystem,python,stacktrace,atomic,chrono,date_time"
./b2 cxxflags="-fPIC" link=static -j "${CPU_COUNT}"
./b2 cxxflags="-fPIC" link=static install
sed -i 's/#if PTHREAD_STACK_MIN > 0/#ifdef PTHREAD_STACK_MIN/g' "${BOOST_INSTALL_DIR}/include/boost/thread/pthread/thread_data.hpp"

cd "${DOWNLOAD_DIR}"
rm -rf "${DOWNLOAD_DIR}/boost"

echo ">> Boost ${BOOST_VERSION} installation completed successfully"
