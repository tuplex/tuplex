#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs Alpine Linux packages and CMake required to build tuplex on Alpine Linux.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
CMAKE_VERSION=${CMAKE_VERSION:-3.27.5}

# Start script.
set -euxo pipefail

# need to run this with root privileges
if [ "$(id -u)" -ne 0 ]; then
  echo "Please run this script with root privileges"
  exit 1
fi

echo ">> Installing Alpine Linux packages and CMake"

# Create download directory
DOWNLOAD_DIR=${WORKDIR}/tuplex-downloads
mkdir -p $DOWNLOAD_DIR

echo ">> Installing apk dependencies"
apk update

apk add --no-cache \
    autoconf automake libtool \
    curl libxml2-dev vim build-base \
    openssl-dev zlib-dev ncurses-dev \
    readline-dev sqlite-dev \
    bzip2-dev expat-dev xz-dev \
    tk-dev libffi-dev wget git \
    curl-dev python3-dev py3-pip \
    openjdk11 ninja \
    linux-headers musl-dev \
    openssh file-dev

echo ">> Installing recent cmake"
# fetch recent cmake & install - detect architecture
ARCH=$(uname -m)
if [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
    CMAKE_ARCH="aarch64"
else
    CMAKE_ARCH="x86_64"
fi
URL=https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz
mkdir -p ${DOWNLOAD_DIR}/cmake && cd ${DOWNLOAD_DIR}/cmake &&
  curl -sSL $URL -o cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz &&
  tar -v -zxf cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz &&
  rm -f cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz &&
  cd cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH} &&
  cp -rp bin/* ${PREFIX}/bin/ &&
  cp -rp share/* ${PREFIX}/share/ &&
  cd / && rm -rf ${DOWNLOAD_DIR}/cmake

export PATH=$PREFIX/bin:$PATH
cmake --version

echo ">> Alpine Linux packages and CMake installation completed successfully"
