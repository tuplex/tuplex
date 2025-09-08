#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs Snappy dependencies required to build tuplex on Alpine Linux.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
SNAPPY_VERSION=${SNAPPY_VERSION:-1.2.2}
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

echo ">> Installing Snappy ${SNAPPY_VERSION}"

# build snappy as static lib
git clone https://github.com/google/snappy.git -b ${SNAPPY_VERSION} && cd snappy && git submodule update --init && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC" -DCMAKE_INSTALL_PREFIX=${PREFIX} .. && make -j ${CPU_COUNT} && make install

echo ">> Snappy ${SNAPPY_VERSION} installation completed successfully"
