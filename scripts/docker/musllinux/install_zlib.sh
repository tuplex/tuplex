#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs zlib-ng dependencies required to build tuplex on Alpine Linux.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
ZLIB_VERSION=${ZLIB_VERSION:-2.1.3}
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

echo ">> Installing zlib-ng ${ZLIB_VERSION}"

# Set LD_LIBRARY_PATH
LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}
export LD_LIBRARY_PATH=$PREFIX/lib:$PREFIX/lib64:$LD_LIBRARY_PATH

# note that zlib defines Z_NULL=0 whereas zlib-ng defines it as NULL, patch aws sdk accordingly
git clone https://github.com/zlib-ng/zlib-ng.git && cd zlib-ng && git checkout tags/${ZLIB_VERSION} && mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_FLAGS="-fPIC" -DZLIB_COMPAT=ON -DCMAKE_INSTALL_PREFIX=${PREFIX} .. && make -j ${CPU_COUNT} && make install

echo ">> zlib-ng ${ZLIB_VERSION} installation completed successfully"
