#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs zstd dependencies required to build tuplex on Alpine Linux.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
ZSTD_VERSION=${ZSTD_VERSION:-1.5.6}
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

echo ">> Installing zstd ${ZSTD_VERSION}"

# Create download directory
DOWNLOAD_DIR=${WORKDIR}/tuplex-downloads
mkdir -p ${DOWNLOAD_DIR}/zstd && cd ${DOWNLOAD_DIR}/zstd \
&& curl -LO https://github.com/facebook/zstd/releases/download/v${ZSTD_VERSION}/zstd-${ZSTD_VERSION}.tar.gz \
&& tar -xzf zstd-${ZSTD_VERSION}.tar.gz \
&& rm zstd-${ZSTD_VERSION}.tar.gz \
&& cd zstd-${ZSTD_VERSION} \
&& make -j${CPU_COUNT} CFLAGS="-O2 -fPIC" CXXFLAGS="-O2 -fPIC" \
&& make install PREFIX=${PREFIX} \
&& make install PREFIX=${PREFIX} MOREFLAGS="-fPIC"

echo ">> zstd ${ZSTD_VERSION} installation completed successfully"
