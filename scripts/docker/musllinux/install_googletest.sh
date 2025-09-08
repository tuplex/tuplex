#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs Google Test dependencies required to build tuplex on Alpine Linux.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
GOOGLETEST_VERSION=${GOOGLETEST_VERSION:-1.14.0}
CC=${CC:-gcc}
CXX=${CXX:-g++}

# Get CPU count for parallel builds
CPU_COUNT=$(( 1 * $( grep '^processor[[:space:]]*:' /proc/cpuinfo | wc -l ) ))

# Start script.
set -euxo pipefail

# need to run this with root privileges
if [ "$(id -u)" -ne 0 ]; then
  echo "Please run this script with root privileges"
  exit 1
fi

echo ">> Installing Google Test ${GOOGLETEST_VERSION}"

git clone https://github.com/google/googletest.git -b v${GOOGLETEST_VERSION} && cd googletest && mkdir build && cd build && cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_CXX_FLAGS="-fPIC" -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} .. && make -j ${CPU_COUNT} && make install

echo ">> Google Test ${GOOGLETEST_VERSION} installation completed successfully"
