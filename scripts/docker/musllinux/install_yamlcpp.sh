#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs YAML-CPP dependencies required to build tuplex on Alpine Linux.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
YAML_CPP_VERSION=${YAML_CPP_VERSION:-0.8.0}
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

echo ">> Installing YAML-CPP ${YAML_CPP_VERSION}"

# Create download directory
DOWNLOAD_DIR=${WORKDIR}/tuplex-downloads
mkdir -p ${DOWNLOAD_DIR}/yamlcpp && cd ${DOWNLOAD_DIR}/yamlcpp \
&& git clone https://github.com/jbeder/yaml-cpp.git yaml-cpp \
&& cd yaml-cpp \
&& git checkout tags/${YAML_CPP_VERSION} \
&& mkdir build && cd build \
&& cmake -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} -DYAML_CPP_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF -DCMAKE_CXX_FLAGS="-fPIC" .. \
&& make -j ${CPU_COUNT} && make install

echo ">> YAML-CPP ${YAML_CPP_VERSION} installation completed successfully"
