#!/bin/sh
# (c) Tuplex team 2017-2023
# Installs AWS SDK C++ and AWS Lambda C++ runtime dependencies required to build tuplex on Alpine Linux.

# Variables needed incl. defaults.
PREFIX=${PREFIX:-/opt}
WORKDIR=${WORKDIR:-/tmp}
AWSSDK_CPP_VERSION=${AWSSDK_CPP_VERSION:-1.11.524}
AWS_LAMBDA_CPP_VERSION=${AWS_LAMBDA_CPP_VERSION:-0.2.10}
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

echo ">> Installing AWS SDK C++ ${AWSSDK_CPP_VERSION} and AWS Lambda C++ runtime ${AWS_LAMBDA_CPP_VERSION}"

# Create download directory
DOWNLOAD_DIR=${WORKDIR}/tuplex-downloads
mkdir -p ${DOWNLOAD_DIR}/aws

# Install AWS SDK C++
echo ">> Installing AWS SDK C++"
cd ${DOWNLOAD_DIR}/aws
git clone --progress --verbose --recurse-submodules https://github.com/aws/aws-sdk-cpp.git
cd aws-sdk-cpp && git checkout tags/${AWSSDK_CPP_VERSION} && sed -i 's/int ret = Z_NULL;/int ret = static_cast<int>(Z_NULL);/g' src/aws-cpp-sdk-core/source/client/RequestCompression.cpp && mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DUSE_OPENSSL=ON -DENABLE_TESTING=OFF -DUSE_CRT_HTTP_CLIENT=ON -DENABLE_UNITY_BUILD=ON -DCPP_STANDARD=17 -DBUILD_SHARED_LIBS=OFF -DBUILD_ONLY="s3;s3-crt;core;lambda;transfer" -DCMAKE_INSTALL_PREFIX=${PREFIX} ..
make -j ${CPU_COUNT}
make install

# Installing AWS Lambda C++ runtime
echo ">> Installing AWS Lambda C++ runtime"
cd ${DOWNLOAD_DIR}/aws
git clone https://github.com/awslabs/aws-lambda-cpp.git
cd aws-lambda-cpp
git fetch && git fetch --tags
git checkout v${AWS_LAMBDA_CPP_VERSION}
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=${PREFIX} ..
make -j${CPU_COUNT} && make install

echo ">> AWS SDK C++ ${AWSSDK_CPP_VERSION} and AWS Lambda C++ runtime ${AWS_LAMBDA_CPP_VERSION} installation completed successfully"
