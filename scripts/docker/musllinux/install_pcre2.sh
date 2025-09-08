#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs PCRE2 dependencies required to build tuplex on Alpine Linux.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
PCRE2_VERSION=${PCRE2_VERSION:-10.45}
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

echo ">> Installing PCRE2 ${PCRE2_VERSION}"

# Create download directory
DOWNLOAD_DIR=${WORKDIR}/tuplex-downloads
mkdir -p ${DOWNLOAD_DIR}/pcre2 && cd ${DOWNLOAD_DIR}/pcre2 \
&& curl -LO https://github.com/PhilipHazel/pcre2/releases/download/pcre2-${PCRE2_VERSION}/pcre2-${PCRE2_VERSION}.zip \
&& unzip pcre2-${PCRE2_VERSION}.zip \
&& rm pcre2-${PCRE2_VERSION}.zip \
&& cd pcre2-${PCRE2_VERSION} \
&& ./configure CFLAGS="-O2 -fPIC" --prefix=${PREFIX} --enable-jit=auto --disable-shared \
&& make -j${CPU_COUNT} && make install

echo ">> PCRE2 ${PCRE2_VERSION} installation completed successfully"
