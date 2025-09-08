#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs Protocol Buffers dependencies required to build tuplex on Alpine Linux.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
PROTOBUF_VERSION=${PROTOBUF_VERSION:-32.0}
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

echo ">> Installing Protocol Buffers ${PROTOBUF_VERSION}"

# Create download directory
DOWNLOAD_DIR=${WORKDIR}/tuplex-downloads
mkdir -p ${DOWNLOAD_DIR}/protobuf && cd ${DOWNLOAD_DIR}/protobuf \
&& git clone -b v${PROTOBUF_VERSION} https://github.com/protocolbuffers/protobuf.git && cd protobuf && git submodule update --init --recursive && mkdir build && cd build && cmake -DCMAKE_CXX_FLAGS="-fPIC" -DCMAKE_CXX_STANDARD=17 -Dprotobuf_BUILD_TESTS=OFF -DCMAKE_INSTALL_PREFIX=${PREFIX} .. && make -j ${CPU_COUNT} && make install

echo ">> Protocol Buffers ${PROTOBUF_VERSION} installation completed successfully"
