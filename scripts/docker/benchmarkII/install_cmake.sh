#!/usr/bin/env bash

apt-get update
apt-get install -y curl

# fetch recent cmake & install
CMAKE_VER_MAJOR=3
CMAKE_VER_MINOR=23
CMAKE_VER_PATCH=2
CMAKE_VER="${CMAKE_VER_MAJOR}.${CMAKE_VER_MINOR}"
CMAKE_VERSION="${CMAKE_VER}.${CMAKE_VER_PATCH}"
URL=https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz
mkdir -p /tmp/build && cd /tmp/build &&
  curl -sSL $URL -o cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz &&
  tar -v -zxf cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz &&
  rm -f cmake-${CMAKE_VERSION}-linux-x86_64.tar.gz &&
  cd cmake-${CMAKE_VERSION}-linux-x86_64 &&
  cp -rp bin/* /usr/local/bin/ &&
  cp -rp share/* /usr/local/share/ &&
  cd / && rm -rf /tmp/build
